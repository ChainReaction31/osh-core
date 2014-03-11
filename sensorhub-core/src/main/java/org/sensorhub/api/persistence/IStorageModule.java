/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License Version
 1.1 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at
 http://www.mozilla.org/MPL/MPL-1.1.html
 
 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.
 
 The Original Code is "SensorHub".
 
 The Initial Developer of the Original Code is Sensia Software LLC.
 <http://www.sensiasoftware.com>. Portions created by the Initial
 Developer are Copyright (C) 2013 the Initial Developer. All Rights Reserved.
 
 Please Contact Alexandre Robin <alex.robin@sensiasoftware.com> for more 
 information.
 
 Contributor(s): 
    Alexandre Robin <alex.robin@sensiasoftware.com>
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.module.IModule;


public interface IStorageModule<ConfigType extends StorageConfig> extends IModule<ConfigType>
{

    /**
     * Opens a storage using the specified configuration
     * @param conf
     */
    public abstract void open() throws StorageException;


    /**
     * Closes the storage, freeing all ressources used
     * @throws IOException
     */
    public abstract void close() throws StorageException;


    /**
     * Cleans up all resources used by this storage, including all
     * persisted data
     * @throws SensorHubException
     */
    public abstract void cleanup() throws StorageException;


    /**
     * Backups storage to specified output stream
     * @param os
     */
    public abstract void backup(OutputStream os);


    /**
     * Restores storage from backup obtained from specified input stream
     * @param is
     */
    public abstract void restore(InputStream is);


    /**
     * Changes the storage behavior on record insertion, update or deletion
     * @param autoCommit true to commit changes automatically when a transactional method is called,
     * false if the commit() method should be called manually to persist changes to storage. 
     */
    public abstract void setAutoCommit(boolean autoCommit);


    /**
     * Retrieves auto-commit state
     * @return
     */
    public abstract boolean isAutoCommit();


    /**
     * Commits all changes generated by transactional methods since the last commit event
     */
    public abstract void commit();


    /**
     * Cancels all changes generated by transactional methods since the last commit event
     */
    public abstract void rollback();
    
    
    /**
     * Synchronizes storage with another storage of the same type (potentially remote)
     * @param storage
     */
    public void sync(IStorageModule<?> storage);

}