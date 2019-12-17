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

import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.IModuleProvider;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.impl.module.JarModuleProvider;
import org.sensorhub.impl.client.sta.STAClient;
import org.sensorhub.impl.client.sta.STAClientConfig;


/**
 * <p>
 * Descriptor of STA client module, needed for automatic discovery by
 * the ModuleRegistry.
 * </p>
 *
 * @author Ian Patterson <cr31.dev@gmail.com>
 * @since Dec. 17, 2019
 */
public class STAClientDescriptor extends JarModuleProvider implements IModuleProvider
{

    @Override
    public String getModuleName()
    {
        return "STA Client";
    }


    @Override
    public String getModuleDescription()
    {
        return "SensorThings client for pushing observations to a remote SensorThings server";
    }


    @Override
    public Class<? extends IModule<?>> getModuleClass()
    {
        return STAClient.class;
    }


    @Override
    public Class<? extends ModuleConfig> getModuleConfigClass()
    {
        return STAClientConfig.class;
    }

}
