/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.client.sta;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import net.opengis.swe.v20.DataBlock;

import org.sensorhub.api.client.ClientException;
import org.sensorhub.api.client.IClientModule;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.module.ModuleEvent;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.sensor.ISensorDataInterface;
import org.sensorhub.api.sensor.ISensorModule;
import org.sensorhub.api.sensor.SensorDataEvent;
import org.sensorhub.api.sensor.SensorEvent;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.comm.RobustIPConnection;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.impl.module.RobustConnection;
import org.sensorhub.impl.security.ClientAuth;
import org.sensorhub.utils.MsgUtils;
import org.vast.ows.OWSException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * <p>
 * Implementation of the SensorThings API client that listens to sensor events and
 * forwards them to an STA server using POST requests on the Observation entity collection<br/>
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Dec 11, 2019
 */
public class STAClient extends AbstractModule<STAClientConfig> implements IClientModule<STAClientConfig>, IEventListener {
    RobustConnection connection;
    ISensorModule<?> sensor;
    String staEndpointUrl;
    String offering;
    Map<ISensorDataInterface, StreamInfo> dataStreams;


    public class StreamInfo {
        long datastreamID;
        long foiID;
        public long lastEventTime = Long.MIN_VALUE;
        public int measPeriodMs = 1000;
        public int errorCount = 0;
        private ThreadPoolExecutor threadPool;
        private HttpURLConnection connection;
        private volatile boolean connecting = false;
        private volatile boolean stopping = false;
    }


    public STAClient() {
        this.dataStreams = new LinkedHashMap<>();
    }


    private void setAuth() {
        ClientAuth.getInstance().setUser(config.staEndpoint.user);
        if (config.staEndpoint.password != null)
            ClientAuth.getInstance().setPassword(config.staEndpoint.password.toCharArray());
    }


    protected String getStaEndpointUrl() {
        setAuth();
        return staEndpointUrl;
    }


    @Override
    public void setConfiguration(STAClientConfig config) {
        super.setConfiguration(config);

        // compute full host URL
        String scheme = "http";
        if (config.staEndpoint.enableTLS)
            scheme = "https";
        if(config.staEndpoint.ignorePort){
            staEndpointUrl = scheme + "://" + config.staEndpoint.remoteHost;
        }else{
            staEndpointUrl = scheme + "://" + config.staEndpoint.remoteHost + ":" + config.staEndpoint.remotePort;
        }
        if (config.staEndpoint.resourcePath != null) {
            if (config.staEndpoint.resourcePath.charAt(0) != '/')
                staEndpointUrl += '/';
            staEndpointUrl += config.staEndpoint.resourcePath;
        }
    }

    ;


    @Override
    public void init() throws SensorHubException {
        // get handle to sensor data source
        try {
            sensor = SensorHub.getInstance().getSensorManager().getModuleById(config.sensorID);
        } catch (Exception e) {
            throw new ClientException("Cannot find sensor with local ID " + config.sensorID, e);
        }

        // create connection handler
        this.connection = new RobustIPConnection(this, config.connection, "STA server") {
            public boolean tryConnect() throws IOException {
                // first check if we can reach remote host on specified port
                // TODO: Might need to mess with config to set reachability requirements
                if (!tryConnectTCP(config.staEndpoint.remoteHost, config.staEndpoint.remotePort))
                    return false;

                // check connection to STA by fetching service root
                try {
                    HttpURLConnection conn = (HttpURLConnection) new URL(staEndpointUrl).openConnection();
                    conn.connect();
                    if (conn.getResponseCode() != 200) {
                        module.reportError("STA service returned " + conn.getResponseCode(), null, true);
                        return false;
                    }
                    conn.disconnect();
                } catch (Exception e) {
                    return false;
                }

                return true;
            }
        };
    }


    @Override
    public void requestStart() throws SensorHubException {
        if (canStart()) {
            try {
                // register to sensor events            
                reportStatus("Waiting for data source " + MsgUtils.moduleString(sensor));
                sensor.registerListener(this);

                // we'll actually start when we receive sensor STARTED event
            } catch (Exception e) {
                reportError(CANNOT_START_MSG, e);
                requestStop();
            }
        }
    }


    @Override
    public void start() throws SensorHubException {
        System.out.println("*****\n Module STA Should be starting. \n *****");
        connection.updateConfig(config.connection);
        connection.waitForConnection();
        reportStatus("Connected to " + getStaEndpointUrl());

        try {
            // register sensor
            registerSensor(sensor);
            getLogger().info("Sensor {} registered with STA", MsgUtils.moduleString(sensor));
        } catch (Exception e) {
            throw new ClientException("Error while registering sensor with remote STA", e);
        }


        // register all stream templates
        for (ISensorDataInterface o : sensor.getAllOutputs().values()) {
            // skip excluded outputs
            if (config.excludedOutputs != null && config.excludedOutputs.contains(o.getName()))
                continue;

            try {
                registerDataStream(o);
            } catch (Exception e) {
                throw new ClientException("Error while registering " + o.getName() + " data stream with remote STA", e);
            }
        }

        getLogger().info("Sensor and Datastreams registered with STA");
        setState(ModuleState.STARTED);
    }


    @Override
    public void stop() throws SensorHubException {
        // cancel reconnection loop
        if (connection != null)
            connection.cancel();

        // unregister from sensor
        if (sensor != null)
            sensor.unregisterListener(this);

        // stop all streams
        for (Entry<ISensorDataInterface, StreamInfo> entry : dataStreams.entrySet())
            stopStream(entry.getKey(), entry.getValue());
    }


    /*
     * Stop listening and pushing data for the given stream
     */
    protected void stopStream(ISensorDataInterface output, StreamInfo streamInfo) {
        // unregister listeners
        output.unregisterListener(this);

        // stop thread pool
        try {
            if (streamInfo.threadPool != null && !streamInfo.threadPool.isShutdown()) {
                streamInfo.threadPool.shutdownNow();
                streamInfo.threadPool.awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    /*
     * Registers sensor with remote STA
     */
    protected void registerSensor(ISensorModule<?> sensor) throws OWSException {
        // don't worry about registering the sensor dynamically for now
        // just create a Sensor on the STA server manually
        // with this tool https://opensensorhub.github.io/project-scira/sensorthings/tester/
    }


    /*
     * Update sensor description at remote STA
     */
    protected void updateSensor(ISensorModule<?> sensor) throws OWSException {
        // no need to support updates for now
    }


    /*
     * Prepare to send the given sensor output data to the remote STA server
     */
    protected void registerDataStream(ISensorDataInterface sensorOutput) throws OWSException {
        // don't worry about registering the datastream dynamically for now
        // just create the Datastream to push to on the STA server manually
        // with this tool https://opensensorhub.github.io/project-scira/sensorthings/tester/

        // add stream info to map
        StreamInfo streamInfo = new StreamInfo();
//        streamInfo.datastreamID = 12; //hard coded datastream ID;
        streamInfo.datastreamID = config.staOptionsConfig.dsID;
        streamInfo.foiID = config.staOptionsConfig.foiID;
        streamInfo.measPeriodMs = (int) (sensorOutput.getAverageSamplingPeriod() * 1000);
        dataStreams.put(sensorOutput, streamInfo);

        // start thread pool
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(config.connection.maxQueueSize);
        streamInfo.threadPool = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, workQueue);

        // send last record
        if (sensorOutput.getLatestRecord() != null)
            send(new SensorDataEvent(
                            sensorOutput.getLatestRecordTime(),
                            sensorOutput, sensorOutput.getLatestRecord()),
                    streamInfo);

        // register to data events
        sensorOutput.registerListener(this);
    }


    @Override
    public void handleEvent(final Event<?> e) {
        // sensor module lifecycle event
        if (e instanceof ModuleEvent) {
            ModuleState newState = ((ModuleEvent) e).getNewState();

            // start when sensor is started
            if (newState == ModuleState.STARTED) {
                try {
                    start();
                } catch (SensorHubException ex) {
                    reportError("Could not start STA Client", ex);
                    setState(ModuleState.STOPPED);
                }
            }
        }

        // sensor description updated
        else if (e instanceof SensorEvent) {
            if (((SensorEvent) e).getType() == SensorEvent.Type.SENSOR_CHANGED) {
                try {
                    updateSensor(sensor);
                } catch (OWSException ex) {
                    getLogger().error("Error when sending updated sensor description to STA", ex);
                }
            }
        }

        // sensor data received
        else if (e instanceof DataEvent) {
            // retrieve stream info
            StreamInfo streamInfo = dataStreams.get(e.getSource());
            if (streamInfo == null)
                return;

            // we stop here if we had too many errors
            if (streamInfo.errorCount >= config.connection.maxConnectErrors) {
                String outputName = ((SensorDataEvent) e).getSource().getName();
                reportError("Too many errors sending '" + outputName + "' data to STA server. Stopping Stream.", null);
                stopStream((ISensorDataInterface) e.getSource(), streamInfo);
                checkDisconnected();
                return;
            }

            // skip if we cannot handle more requests
            if (streamInfo.threadPool.getQueue().remainingCapacity() == 0) {
                String outputName = ((SensorDataEvent) e).getSource().getName();
                getLogger().warn("Too many '{}' records to send to STA server. Bandwidth cannot keep up.", outputName);
                getLogger().info("Skipping records by purging record queue");
                streamInfo.threadPool.getQueue().clear();
                return;
            }

            // record last event time
            streamInfo.lastEventTime = e.getTimeStamp();

            // send record using one of 2 methods
            send((SensorDataEvent) e, streamInfo);
        }
    }


    private void checkDisconnected() {
        // if all streams have been stopped, initiate reconnection
        boolean allStopped = true;
        for (StreamInfo streamInfo : dataStreams.values()) {
            if (!streamInfo.stopping) {
                allStopped = false;
                break;
            }
        }

        if (allStopped) {
            reportStatus("All streams stopped on error. Trying to reconnect...");
            connection.reconnect();
        }
    }


    /*
     * Sends each new record using POST request to the Observation entity collection
     */
    private void send(final SensorDataEvent e, final StreamInfo streamInfo) {
        System.out.println(e.getRecordDescription().toString());
        DataBlock dataBlock = e.getRecords()[0];

        // create send request task
        Runnable sendTask = new Runnable() {
            @Override
            public void run() {
                try {
                    String obsCollectionUrl = staEndpointUrl + "/Observations";
                    HttpURLConnection cnx = (HttpURLConnection) new URL(staEndpointUrl).openConnection();
                    cnx.setDoOutput(true);
                    cnx.setRequestMethod("POST");
                    cnx.setRequestProperty("Content-Type", "application/json");
                    cnx.connect();

                    try (JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(cnx.getOutputStream()))) {
                        // This is where you put your custom code to serialize as JSON and
                        // send it in the POST request body
                        jsonWriter.beginObject();

                        // I think we may be able to use a single DataStream in this nonstandard approach for SCIRA...
//                        jsonWriter.name("MultiDatastream").beginObject()
//                                .name("@iot.id")
//                                .value(streamInfo.datastreamID)
//                                .endObject();
                        System.out.println(streamInfo.datastreamID);
                        jsonWriter.name("Datastream").beginObject().name("@iot.id").value(streamInfo.datastreamID).endObject();
                        jsonWriter.name("FeatureOfInterest").beginObject().name("@iot.id").value(streamInfo.foiID).endObject();

                        // Get Time Record from event data
                        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date(e.getRecords()[0].getIntValue(0) * 1000L));
                        //jsonWriter.name("phenomenonTime").value(e.getRecords()[0].getStringValue(0));
//                        jsonWriter.name("phenomenonTime").value("isodate");
                        jsonWriter.name("resultTime").value(timestamp);
//                        jsonWriter.name("resultTime").value("Same as phenomenonTime");





                        //StringWriter stringWriter = new StringWriter();
                        //JsonWriter result = new JsonWriter(stringWriter);
//                        JsonObject result = new JsonObject();


//                        jsonWriter.name("result").beginObject();
                        // result.beginObject();
//                        result.name("id").value(e.getRecords()[0].getStringValue(1));
//                        result.add("id", e.getRecords()[0].getStringValue(1));
//                        result.name("timestamp").value(e.getRecords()[0].getStringValue(0));
                        
                        String resultString = "";
                        if (e.getSource().getName().contains("Street Closure")) {
                            String rID = "\"id\":\"" + e.getRecords()[0].getStringValue(1) + "\",";
//                            String rTimestamp = "\"timeStamp\":\"" + e.getRecords()[0].getStringValue(0) + "\",";
                            String rTimestamp = "\"timeStamp\":\"" + timestamp + "\",";
                            String rOBSType = "\"observationType\":\"streetclosure\",";
                            String rParams = "\"params\":{";
                            String rpClosureAction = "\"closureAction\":\"" + e.getRecords()[0].getStringValue(7) + "\",";
                            String rpClosureType = "\"closureType\":\"" + e.getRecords()[0].getStringValue(6) + "\",";
                            String rpLocation = "\"location\":{";
                            String rplType = "\"type\":\"Feature\",";
                            String rplGeometry = "\"geometry\":{";
                            String rplgType = "\"type\":\"Circle\",";
                            String rplgCoordinates = "\"coordinates\":[" + e.getRecords()[0].getDoubleValue(2) + "," + e.getRecords()[0].getDoubleValue(3) + "],";
                            String rplgRadius = "\"radius\":" + e.getRecords()[0].getDoubleValue(5) + ",";
                            String rplgProperties = "\"properties\":{\"radius_units\":\"ft\"}";
                            String rplgeomEnd = "}";
                            String rplocEnd = "}";
                            String rparamsEnd = "},";
                            String rEncodingType = "\"encodingType\":\"application/vnd.geo+json\"";

                            resultString = "{" + rID + rTimestamp + rOBSType + rParams + rpClosureAction + rpClosureType + rpLocation + rplType + rplGeometry + rplgType + rplgCoordinates + rplgRadius + rplgProperties + rplgeomEnd + rplocEnd + rparamsEnd + rEncodingType + "}";

//                            result.name("result").value(resultString);

                            /*result.name("observationType").value("streetClosure");

                            result.name("params").beginObject();
//                            result.name("closureAction").value(e.getRecords()[7].getStringValue());
                            result.name("closureAction").value(e.getRecords()[0].getStringValue(7));
                            result.name("closureType").value(e.getRecords()[0].getStringValue(6));

                            result.name("location").beginObject();
                            result.name("type").value("Feature");

                            result.name("geometry").beginObject();
                            result.name("type").value("Circle");  // Always Circle in App right now
                            result.name("coordinates").beginArray()
                                    .value(e.getRecords()[0].getDoubleValue(2))
                                    .value(e.getRecords()[0].getDoubleValue(3))
                                    .endArray();
                            result.name("radius").value(e.getRecords()[0].getDoubleValue(5));
                            result.name("properties").beginObject();
                            result.name("radius_units").value("ft");
                            result.endObject().endObject().endObject().endObject();
                            // End Params
                            result.name("encodingType").value("application/vnd.geo+json");
                            result.endObject();*/
                    }else if(e.getSource().getName().contains("Aid")){
                            String rID = "\"id\":\"" + e.getRecords()[0].getStringValue(1) + "\",";
                            String rTimestamp = "\"timeStamp\":\"" + timestamp + "\",";
                            String rOBSType = "\"observationType\":\"aid\",";
                            String rParams = "\"params\":{";
                            String rpAidType = "\"aidType\":\"" + e.getRecords()[0].getStringValue(6) + "\",";
                            String rpAidPersons = "\"aidPersons\":\"" + e.getRecords()[0].getStringValue(7) + "\",";
                            String rpUrgency = "\"urgency\":\"" + e.getRecords()[0].getStringValue(8) + "\",";
                            String rpDescription = "\"description\":\"" + e.getRecords()[0].getStringValue(9) + "\",";
                            String rpReporter = "\"reporter\":\"" + e.getRecords()[0].getStringValue(10) + "\",";
                            String rpLocation = "\"location\":{";
                            String rplType = "\"type\":\"Feature\",";
                            String rplGeometry = "\"geometry\":{";
                            String rplgType = "\"type\":\"Circle\",";
                            String rplgCoordinates = "\"coordinates\":[" + e.getRecords()[0].getDoubleValue(2) + "," + e.getRecords()[0].getDoubleValue(3) + "],";
                            String rplgRadius = "\"radius\":" + e.getRecords()[0].getDoubleValue(5) + ",";
                            String rplgProperties = "\"properties\":{\"radius_units\":\"ft\"}";
                            String rplgeomEnd = "}";
                            String rplocEnd = "}";
                            String rparamsEnd = "},";
                            String rEncodingType = "\"encodingType\":\"application/vnd.geo+json\"";

                            resultString = "{" + rID + rTimestamp + rOBSType + rParams + rpAidType + rpAidPersons + rpUrgency + rpDescription + rpReporter + rpLocation + rplType + rplGeometry + rplgType + rplgCoordinates + rplgRadius + rplgProperties + rplgeomEnd + rplocEnd + rparamsEnd + rEncodingType + "}";
                            /*result.name("observationType").value("aid");

                            result.name("params").beginObject();
                            result.name("closureAction").value(e.getRecords()[0].getStringValue());
                            result.name("closureType").value(e.getRecords()[0].getStringValue());

                            result.name("location").beginObject();
                            result.name("type").value("Feature");

                            result.name("geometry").beginObject();
                            result.name("type").value(e.getRecords()[0].getStringValue());
                            result.name("coordinates").beginArray()
                                    .value(e.getRecords()[0].getDoubleValue())
                                    .value(e.getRecords()[0].getDoubleValue())
                                    .endArray();
                            result.name("radius").value(e.getRecords()[0].getDoubleValue());
                            result.name("properties").beginObject();
                            result.name("radius_units").value("ft");
                            result.endObject().endObject().endObject().endObject().endObject();
                            // End Params
                            result.name("encodingType").value("application/vnd.geo+json");*/
                    }
                        else if (e.getSource().getName().contains("Flooding")) {
                            String rID = "\"id\":\"" + e.getRecords()[0].getStringValue(1) + "\",";
                            String rTimestamp = "\"timeStamp\":\"" + timestamp + "\",";
                            String rOBSType = "\"observationType\":\"flooding\",";
                            String rParams = "\"params\":{";
                            String rpFeatureType = "\"featureType\":\"" + e.getRecords()[0].getStringValue(6) + "\",";
                            String rpObsMode = "\"obsMode\":\"" + e.getRecords()[0].getStringValue(8) + "\",";
                            String rpObsDepth = "\"obsDepth\":\"" + e.getRecords()[0].getStringValue(7) + "\",";
                            String rpObsTime = "\"obsTime\":\"" + e.getRecords()[0].getStringValue(0) + "\",";
                            String rpValidTime = "\"validTime\":\"" + e.getRecords()[0].getStringValue(0) + "\",";
                            String rpLocation = "\"location\":{";
                            String rplType = "\"type\":\"Feature\",";
                            String rplGeometry = "\"geometry\":{";
                            String rplgType = "\"type\":\"Circle\",";
                            String rplgCoordinates = "\"coordinates\":[" + e.getRecords()[0].getDoubleValue(2) + "," + e.getRecords()[0].getDoubleValue(3) + "],";
                            String rplgRadius = "\"radius\":" + e.getRecords()[0].getDoubleValue(5) + ",";
                            String rplgProperties = "\"properties\":{\"radius_units\":\"ft\"}";
                            String rplgeomEnd = "}";
                            String rplocEnd = "}";
                            String rparamsEnd = "},";
                            String rEncodingType = "\"encodingType\":\"application/vnd.geo+json\"";

                            resultString = "{" + rID + rTimestamp + rOBSType + rParams + rpFeatureType + rpObsMode + rpObsDepth + rpObsTime + rpValidTime + rpLocation + rplType + rplGeometry + rplgType + rplgCoordinates + rplgRadius + rplgProperties + rplgeomEnd + rplocEnd + rparamsEnd + rEncodingType + "}";

                    } else if (e.getSource().getName().contains("Medical")) {
                            String rID = "\"id\":\"" + e.getRecords()[0].getStringValue(1) + "\",";
                            String rTimestamp = "\"timeStamp\":\"" + timestamp + "\",";
                            String rOBSType = "\"observationType\":\"med\",";
                            String rParams = "\"params\":{";
                            String rpMedType = "\"medType\":\"" + e.getRecords()[0].getStringValue(6) + "\",";
                            String rpAction = "\"action\":\"" + "open" + "\",";
                            String rpMedSign = "\"medSign\":\"" + "unknown" + "\",";
                            String rpValue = "\"value\":\"" + e.getRecords()[0].getStringValue(7) + "\",";
                            String rpLocation = "\"location\":{";
                            String rplType = "\"type\":\"Feature\",";
                            String rplGeometry = "\"geometry\":{";
                            String rplgType = "\"type\":\"Circle\",";
                            String rplgCoordinates = "\"coordinates\":[" + e.getRecords()[0].getDoubleValue(2) + "," + e.getRecords()[0].getDoubleValue(3) + "],";
                            String rplgRadius = "\"radius\":" + e.getRecords()[0].getDoubleValue(5) + ",";
                            String rplgProperties = "\"properties\":{\"radius_units\":\"ft\"}";
                            String rplgeomEnd = "}";
                            String rplocEnd = "}";
                            String rparamsEnd = "},";
                            String rEncodingType = "\"encodingType\":\"application/vnd.geo+json\"";

                            resultString = "{" + rID + rTimestamp + rOBSType + rParams + rpMedType + rpAction + rpMedSign + rpValue + rpLocation + rplType + rplGeometry + rplgType + rplgCoordinates + rplgRadius + rplgProperties + rplgeomEnd + rplocEnd + rparamsEnd + rEncodingType + "}";
                    } else if (e.getSource().getName().contains("Tracking")) {
                            String rID = "\"id\":\"" + e.getRecords()[0].getStringValue(1) + "\",";
                            String rTimestamp = "\"timeStamp\":\"" + timestamp + "\",";
                            String rOBSType = "\"observationType\":\"track\",";
                            String rConfidence = "\"confidence\":\"" + e.getRecords()[0].getStringValue(5) + "\",";
                            String rParams = "\"params\":{";
                            String rpAssetId = "\"assetid\":\"" + e.getRecords()[0].getStringValue(7) + "\",";
                            String rpGPSTimestamp = "\"gpstimestamp\":\"" + e.getRecords()[0].getStringValue(0) + "\",";
                            String rpTrackMethod = "\"trackMethod\":\"" + e.getRecords()[0].getStringValue(9) + "\",";
                            String rpLocation = "\"location\":{";
                            String rplType = "\"type\":\"Feature\",";
                            String rplGeometry = "\"geometry\":{";
                            String rplgType = "\"type\":\"Point\",";
                            String rplgCoordinates = "\"coordinates\":[" + e.getRecords()[0].getDoubleValue(2) + "," + e.getRecords()[0].getDoubleValue(3) + "]";
                            //String rplgRadius = "\"radius\":" + e.getRecords()[0].getDoubleValue(5) + ",";
                            //String rplgProperties = "\"properties\":{\"radius_units\":\"ft\"}";
                            String rplgeomEnd = "}";
                            String rplocEnd = "}";
                            String rparamsEnd = "},";
                            String rEncodingType = "\"encodingType\":\"application/vnd.geo+json\"";

                            resultString = "{" + rID + rTimestamp + rOBSType + rConfidence + rParams + rpAssetId + rpGPSTimestamp + rpTrackMethod + rpLocation + rplType + rplGeometry + rplgType + rplgCoordinates + rplgeomEnd + rplocEnd + rparamsEnd + rEncodingType + "}";
                    }
//                    String resultString = result.;
                    System.out.println(resultString);
                    jsonWriter.name("result").value(resultString);
                    jsonWriter.endObject();
                	//jsonWriter.
//                    System.out.println(cnx.getResponseMessage());
                }

                    InputStream is = cnx.getInputStream();
                    System.out.println("Printing Request---");
                    InputStreamReader isReader = new InputStreamReader(is);
                    BufferedReader reader = new BufferedReader(isReader);
                    StringBuffer sb = new StringBuffer();
                    String isString;
                    while((isString = reader.readLine()) != null){
                        sb.append(isString);
                    }
                    System.out.println(sb.toString());
                    //System.out.println(cnx.getOutputStream().toString());

            }
                catch(Exception ex)
            {
                String outputName = e.getSource().getName();
                reportError("Error when sending '" + outputName + "' data to STA server", ex, true);
                streamInfo.errorCount++;
            }
        }
    };

    // run task in async thread pool
        streamInfo.threadPool.execute(sendTask);
}


    @Override
    public boolean isConnected() {
        return connection.isConnected();
    }


    public Map<ISensorDataInterface, StreamInfo> getDataStreams() {
        return dataStreams;
    }


    @Override
    public void cleanup() throws SensorHubException {
        // nothing to clean
    }
}
