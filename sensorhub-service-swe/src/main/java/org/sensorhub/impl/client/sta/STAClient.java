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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
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
public class STAClient extends AbstractModule<STAClientConfig> implements IClientModule<STAClientConfig>, IEventListener
{
    RobustConnection connection;
    ISensorModule<?> sensor;
    String staEndpointUrl;
    String offering;
    Map<ISensorDataInterface, StreamInfo> dataStreams;
    
    
    public class StreamInfo
    {
        long datastreamID;
        public long lastEventTime = Long.MIN_VALUE;
        public int measPeriodMs = 1000;
        public int errorCount = 0;
        private ThreadPoolExecutor threadPool;
        private HttpURLConnection connection;
        private volatile boolean connecting = false;
        private volatile boolean stopping = false;
    }
    
    
    public STAClient()
    {
        this.dataStreams = new LinkedHashMap<>();
    }
    
    
    private void setAuth()
    {
        ClientAuth.getInstance().setUser(config.staEndpoint.user);
        if (config.staEndpoint.password != null)
            ClientAuth.getInstance().setPassword(config.staEndpoint.password.toCharArray());
    }


    protected String getStaEndpointUrl()
    {
        setAuth();
        return staEndpointUrl;
    }
    
    
    @Override
    public void setConfiguration(STAClientConfig config)
    {
        super.setConfiguration(config);
         
        // compute full host URL
        String scheme = "http";
        if (config.staEndpoint.enableTLS)
            scheme = "https";
        staEndpointUrl = scheme + "://" + config.staEndpoint.remoteHost + ":" + config.staEndpoint.remotePort;
        if (config.staEndpoint.resourcePath != null)
        {
            if (config.staEndpoint.resourcePath.charAt(0) != '/')
                staEndpointUrl += '/';
            staEndpointUrl += config.staEndpoint.resourcePath;
        }
    };
    
    
    @Override
    public void init() throws SensorHubException
    {
        // get handle to sensor data source
        try
        {
            sensor = SensorHub.getInstance().getSensorManager().getModuleById(config.sensorID);
        }
        catch (Exception e)
        {
            throw new ClientException("Cannot find sensor with local ID " + config.sensorID, e);
        }
        
        // create connection handler
        this.connection = new RobustIPConnection(this, config.connection, "STA server")
        {
            public boolean tryConnect() throws IOException
            {
                // first check if we can reach remote host on specified port
                if (!tryConnectTCP(config.staEndpoint.remoteHost, config.staEndpoint.remotePort))
                    return false;
                
                // check connection to STA by fetching service root
                try
                {
                    HttpURLConnection conn = (HttpURLConnection)new URL(staEndpointUrl).openConnection();
                    conn.connect();
                    if (conn.getResponseCode() != 200) {
                        module.reportError("STA service returned " + conn.getResponseCode(), null, true);
                        return false;
                    }
                    conn.disconnect();
                }
                catch (Exception e)
                {
                    return false;
                }

                return true;
            }
        };
    }
    
    
    @Override
    public void requestStart() throws SensorHubException
    {
        if (canStart())
        {
            try
            {
                // register to sensor events            
                reportStatus("Waiting for data source " + MsgUtils.moduleString(sensor));
                sensor.registerListener(this);                
                
                // we'll actually start when we receive sensor STARTED event
            }
            catch (Exception e)
            {
                reportError(CANNOT_START_MSG, e);
                requestStop();
            }
        }
    }
    
    
    @Override
    public void start() throws SensorHubException
    {
        connection.updateConfig(config.connection);
        connection.waitForConnection();
        reportStatus("Connected to " + getStaEndpointUrl());
        
        try
        {   
            // register sensor
            registerSensor(sensor);
            getLogger().info("Sensor {} registered with STA", MsgUtils.moduleString(sensor));
        }
        catch (Exception e)
        {
            throw new ClientException("Error while registering sensor with remote STA", e);
        }
        
        
        // register all stream templates
        for (ISensorDataInterface o: sensor.getAllOutputs().values())
        {
            // skip excluded outputs
            if (config.excludedOutputs != null && config.excludedOutputs.contains(o.getName()))
                continue;
            
            try
            {
                registerDataStream(o);
            }
            catch (Exception e)
            {
                throw new ClientException("Error while registering " + o.getName() + " data stream with remote STA", e);
            }
        }
        
        getLogger().info("Sensor and Datastreams registered with STA");
        setState(ModuleState.STARTED);        
    }
    
    
    @Override
    public void stop() throws SensorHubException
    {
        // cancel reconnection loop
        if (connection != null)
            connection.cancel();
        
        // unregister from sensor
        if (sensor != null)
            sensor.unregisterListener(this);
        
        // stop all streams
        for (Entry<ISensorDataInterface, StreamInfo> entry: dataStreams.entrySet())
            stopStream(entry.getKey(), entry.getValue());
    }
    
    
    /*
     * Stop listening and pushing data for the given stream
     */
    protected void stopStream(ISensorDataInterface output, StreamInfo streamInfo)
    {
        // unregister listeners
        output.unregisterListener(this);
        
        // stop thread pool
        try
        {
            if (streamInfo.threadPool != null && !streamInfo.threadPool.isShutdown())
            {
                streamInfo.threadPool.shutdownNow();
                streamInfo.threadPool.awaitTermination(3, TimeUnit.SECONDS);
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
    
    
    /*
     * Registers sensor with remote STA
     */
    protected void registerSensor(ISensorModule<?> sensor) throws OWSException
    {
        // don't worry about registering the sensor dynamically for now
        // just create a Sensor on the STA server manually
        // with this tool https://opensensorhub.github.io/project-scira/sensorthings/tester/
    }
    
    
    /*
     * Update sensor description at remote STA
     */
    protected void updateSensor(ISensorModule<?> sensor) throws OWSException
    {
        // no need to support updates for now
    }
    
    
    /*
     * Prepare to send the given sensor output data to the remote STA server
     */
    protected void registerDataStream(ISensorDataInterface sensorOutput) throws OWSException
    {
        // don't worry about registering the datastream dynamically for now
        // just create the Datastream to push to on the STA server manually
        // with this tool https://opensensorhub.github.io/project-scira/sensorthings/tester/
        
        // add stream info to map
        StreamInfo streamInfo = new StreamInfo();
        streamInfo.datastreamID = 12; //hard coded datastream ID;
        streamInfo.measPeriodMs = (int)(sensorOutput.getAverageSamplingPeriod()*1000);
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
    public void handleEvent(final Event<?> e)
    {
        // sensor module lifecycle event
        if (e instanceof ModuleEvent)
        {
            ModuleState newState = ((ModuleEvent) e).getNewState();
            
            // start when sensor is started
            if (newState == ModuleState.STARTED)
            {
                try
                {
                    start();
                }
                catch (SensorHubException ex)
                {
                    reportError("Could not start STA Client", ex);
                    setState(ModuleState.STOPPED);
                }
            }
        }
                
        // sensor description updated
        else if (e instanceof SensorEvent)
        {
            if (((SensorEvent) e).getType() == SensorEvent.Type.SENSOR_CHANGED)
            {
                try
                {
                    updateSensor(sensor);
                }
                catch (OWSException ex)
                {
                    getLogger().error("Error when sending updated sensor description to STA", ex);
                }
            }
        }
        
        // sensor data received
        else if (e instanceof DataEvent)
        {
            // retrieve stream info
            StreamInfo streamInfo = dataStreams.get(e.getSource());
            if (streamInfo == null)
                return;
            
            // we stop here if we had too many errors
            if (streamInfo.errorCount >= config.connection.maxConnectErrors)
            {
                String outputName = ((SensorDataEvent)e).getSource().getName();
                reportError("Too many errors sending '" + outputName + "' data to STA server. Stopping Stream.", null);
                stopStream((ISensorDataInterface)e.getSource(), streamInfo);
                checkDisconnected();                
                return;
            }
            
            // skip if we cannot handle more requests
            if (streamInfo.threadPool.getQueue().remainingCapacity() == 0)
            {
                String outputName = ((SensorDataEvent)e).getSource().getName();
                getLogger().warn("Too many '{}' records to send to STA server. Bandwidth cannot keep up.", outputName);
                getLogger().info("Skipping records by purging record queue");
                streamInfo.threadPool.getQueue().clear();
                return;
            }
            
            // record last event time
            streamInfo.lastEventTime = e.getTimeStamp();
            
            // send record using one of 2 methods
            send((SensorDataEvent)e, streamInfo);
        }
    }
    
    
    private void checkDisconnected()
    {
        // if all streams have been stopped, initiate reconnection
        boolean allStopped = true;
        for (StreamInfo streamInfo: dataStreams.values())
        {
            if (!streamInfo.stopping)
            {
                allStopped = false;
                break;
            }
        }
        
        if (allStopped)
        {
            reportStatus("All streams stopped on error. Trying to reconnect...");
            connection.reconnect();
        }
    }
    
    
    /*
     * Sends each new record using POST request to the Observation entity collection
     */
    private void send(final SensorDataEvent e, final StreamInfo streamInfo)
    {
        // create send request task
        Runnable sendTask = new Runnable() {
            @Override
            public void run()
            {
                try
                {
                    for (DataBlock data: e.getRecords())
                    {
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

                            jsonWriter.name("MultiDatastream").beginObject()
                                    .name("@iot.id")
                                    .value(streamInfo.datastreamID)
                                    .endObject();

                            jsonWriter.name("phenomenonTime").value("isodate");
                            jsonWriter.name("resultTime").value("same as phenomenonTime");
                            jsonWriter.name("result").beginArray()
                                    .value(data.getDoubleValue(0))
                                    .value(data.getStringValue(1))
                                    .value(data.getIntValue(2))
                                    .endArray();

                            jsonWriter.endObject();
                        }
                    }
                }
                catch (Exception ex)
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
    public boolean isConnected()
    {
        return connection.isConnected();
    }
    
    
    public Map<ISensorDataInterface, StreamInfo> getDataStreams()
    {
        return dataStreams;
    }


    @Override
    public void cleanup() throws SensorHubException
    {
        // nothing to clean
    }
}
