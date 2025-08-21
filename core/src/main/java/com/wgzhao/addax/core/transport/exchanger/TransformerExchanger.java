/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.transport.exchanger;

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.transport.transformer.TransformerExecution;
import com.wgzhao.addax.core.util.container.ClassLoaderSwapper;

import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

/**
 * no comments.
 * Created by liqiang on 16/3/9.
 */
public abstract class TransformerExchanger
{

    protected final TaskPluginCollector pluginCollector;

    protected final int taskGroupId;
    protected final int taskId;
    protected final Communication currentCommunication;
    private final List<TransformerExecution> transformerExecs;
    private final ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
    private long totalExhaustedTime = 0;
    private long totalFilterRecords = 0;
    private long totalSuccessRecords = 0;
    private long totalFailedRecords = 0;

    public TransformerExchanger(int taskGroupId, int taskId, Communication communication,
            List<TransformerExecution> transformerExecs,
            TaskPluginCollector pluginCollector)
    {

        this.transformerExecs = transformerExecs;
        this.pluginCollector = pluginCollector;
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
        this.currentCommunication = communication;
    }

    public Record doTransformer(Record record)
    {
        if (transformerExecs == null || transformerExecs.isEmpty()) {
            return record;
        }

        Record result = record;

        long diffExhaustedTime = 0;
        String errorMsg = null;
        boolean failed = false;
        for (TransformerExecution transformerInfoExec : transformerExecs) {
            long startTs = System.nanoTime();

            if (transformerInfoExec.getClassLoader() != null) {
                classLoaderSwapper.setCurrentThreadClassLoader(transformerInfoExec.getClassLoader());
            }

            /*
             * Deferred validation of transformer parameters; throw directly if invalid rather than marking dirty.
             * No need to validate parameters inside plugins except for plugin-specific ones like parameter count.
             */
            if (!transformerInfoExec.isChecked()) {

                if (transformerInfoExec.getColumnIndex() != null
                        && transformerInfoExec.getColumnIndex() >= record.getColumnNumber()) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("columnIndex[%s] out of bound[%s]. name=%s",
                                    transformerInfoExec.getColumnIndex(), record.getColumnNumber(),
                                    transformerInfoExec.getTransformerName()));
                }
                transformerInfoExec.setIsChecked(true);
            }

            try {
                result = transformerInfoExec
                        .getTransformer()
                        .evaluate(result, transformerInfoExec.getContext(),
                                transformerInfoExec.getFinalParas());
            }
            catch (Exception e) {
                errorMsg = String.format("The transformer(%s) has encountered an exception(%s)",
                        transformerInfoExec.getTransformerName(),
                        e.getMessage());
                failed = true;
                break;
            }
            finally {
                if (transformerInfoExec.getClassLoader() != null) {
                    classLoaderSwapper.restoreCurrentThreadClassLoader();
                }
            }

            if (result == null) {
                totalFilterRecords++;
                break;
            }

            long diff = System.nanoTime() - startTs;
            diffExhaustedTime += diff;
        }

        totalExhaustedTime += diffExhaustedTime;

        if (failed) {
            totalFailedRecords++;
            this.pluginCollector.collectDirtyRecord(record, errorMsg);
            return null;
        }
        else {
            totalSuccessRecords++;
            return result;
        }
    }

    public void doStat()
    {
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS, totalSuccessRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS, totalFailedRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS, totalFilterRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_USED_TIME, totalExhaustedTime);
    }
}
