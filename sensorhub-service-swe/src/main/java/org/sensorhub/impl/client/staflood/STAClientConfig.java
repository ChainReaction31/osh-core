/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.client.staflood;

import org.sensorhub.api.client.ClientConfig;
import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.config.DisplayInfo.FieldType;
import org.sensorhub.api.config.DisplayInfo.FieldType.Type;
import org.sensorhub.api.config.DisplayInfo.ModuleType;
import org.sensorhub.api.config.DisplayInfo.Required;
import org.sensorhub.api.sensor.ISensorModule;
import org.sensorhub.impl.comm.HTTPConfig;
import org.sensorhub.impl.comm.RobustIPConnectionConfig;

import java.util.ArrayList;
import java.util.List;


/**
 * <p>
 * Configuration class for the SensorThings API client module
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Dec 11, 2019
 */
public class STAClientConfig extends ClientConfig
{
    @DisplayInfo(desc="Local ID of sensor to register with SOS")
    @FieldType(Type.MODULE_ID)
    @ModuleType(ISensorModule.class)
    @Required
    public String sensorID;
    
    
    @DisplayInfo(desc="Names of outputs that should not be pushed to remote SOS server")
    public List<String> excludedOutputs = new ArrayList<>();
    
    
    @DisplayInfo(label="STA Endpoint", desc="STA endpoint where the requests are sent")
    public STAHTTPConfig staEndpoint = new STAHTTPConfig();
//    public HTTPConfig staEndpoint = new HTTPConfig();
    
    
    @DisplayInfo(label="Connection Options")
    public STAConnectionConfig connection = new STAConnectionConfig();

    @DisplayInfo(label = "STA Options")
    public STAOptionsConfig staOptionsConfig = new STAOptionsConfig();
    
    
    public static class STAConnectionConfig extends RobustIPConnectionConfig
    {
        @DisplayInfo(desc="Maximum number of records in upload queue (used to compensate for variable bandwidth)")
        public int maxQueueSize = 10;

        
        @DisplayInfo(desc="Maximum number of stream errors before we try to reconnect to remote server")
        public int maxConnectErrors = 10;
    }

    public static class STAOptionsConfig
    {
        @DisplayInfo(label="Sensor ID", desc = "Numerical ID of the Sensor")
        public int sensorID = 0;

        @DisplayInfo(label="DataStream ID", desc = "Numerical ID of the DataStream")
        public int dsID = 0;

        @DisplayInfo(label="FOI ID", desc = "Numerical ID of the Feature of Interest")
        public int foiID = 0;


//        @DisplayInfo(label="Street Closure DataStream ID", desc = "Numerical ID of the DataStream")
//        public int street_dsID = 0;
//
//        @DisplayInfo(label="Flooding DataStream ID", desc = "Numerical ID of the DataStream")
//        public int flooding_dsID = 0;
//
//        @DisplayInfo(label="Medical DataStream ID", desc = "Numerical ID of the DataStream")
//        public int medical_dsID = 0;
//
//        @DisplayInfo(label="Aid DataStream ID", desc = "Numerical ID of the DataStream")
//        public int aid_dsID = 0;
//
//        @DisplayInfo(label="Track DataStream ID", desc = "Numerical ID of the DataStream")
//        public int track_dsID = 0;

    }

    public static class STAHTTPConfig extends HTTPConfig{
        @DisplayInfo(desc="Ignore Port Number")
        public boolean ignorePort;
    }
    
    
    public STAClientConfig()
    {
        this.moduleClass = STAClientFlood.class.getCanonicalName();
        this.staEndpoint.resourcePath = "/sensorhub/sta/v1.0";
    }
}
