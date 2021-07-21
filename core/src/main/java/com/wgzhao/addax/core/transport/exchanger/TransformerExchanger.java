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

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.transport.transformer.TransformerErrorCode;
import com.wgzhao.addax.core.transport.transformer.TransformerExecution;
import com.wgzhao.addax.core.util.container.ClassLoaderSwapper;

import java.util.List;

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
    private final ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
            .newCurrentThreadClassLoaderSwapper();
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
             * 延迟检查transformer参数的有效性，直接抛出异常，不作为脏数据
             * 不需要在插件中检查参数的有效性。但参数的个数等和插件相关的参数，在插件内部检查
             */
            if (!transformerInfoExec.isChecked()) {

                if (transformerInfoExec.getColumnIndex() != null
                        && transformerInfoExec.getColumnIndex() >= record.getColumnNumber()) {
                    throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
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
                errorMsg = String.format("transformer(%s) has Exception(%s)",
                        transformerInfoExec.getTransformerName(),
                        e.getMessage());
                failed = true;
                //LOG.error(errorMsg, e);
                // transformerInfoExec.addFailedRecords(1);
                //脏数据不再进行后续transformer处理，按脏数据处理，并过滤该record。
                break;
            }
            finally {
                if (transformerInfoExec.getClassLoader() != null) {
                    classLoaderSwapper.restoreCurrentThreadClassLoader();
                }
            }

            if (result == null) {
                /*
                 * 这个null不能传到writer，必须消化掉
                 */
                totalFilterRecords++;
                //transformerInfoExec.addFilterRecords(1);
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

        /*
         * todo 对于多个transformer时，各个transformer的单独统计进行显示。最后再汇总整个transformer的时间消耗.
         * 暂时不统计。
         */
//        if (transformers.size() > 1) {
//            for (transformerInfoExec transformerInfoExec : transformers) {
//                currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_NAME_PREFIX + transformerInfoExec.getTransformerName(), transformerInfoExec.getExaustedTime());
//            }
//        }
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS, totalSuccessRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS, totalFailedRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS, totalFilterRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_USED_TIME, totalExhaustedTime);
    }
}
