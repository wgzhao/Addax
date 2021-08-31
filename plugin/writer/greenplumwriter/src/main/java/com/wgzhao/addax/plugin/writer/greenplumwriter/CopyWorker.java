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

package com.wgzhao.addax.plugin.writer.greenplumwriter;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.util.WriterUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CopyWorker
        implements Callable<Long>
{
    private static final Logger LOG = LoggerFactory.getLogger(CopyWorker.class);
    private final CopyWriterTask task;
    private final Connection connection;
    private final LinkedBlockingQueue<byte[]> queue;
    private final FutureTask<Long> copyResult;
    private final String sql;
    private final PipedInputStream pipeIn;
    private final PipedOutputStream pipeOut;
    private final Thread copyBackendThread;

    public CopyWorker(CopyWriterTask task, String copySql, LinkedBlockingQueue<byte[]> queue)
            throws IOException
    {
        this.task = task;
        this.connection = task.createConnection();
        this.queue = queue;
        this.pipeOut = new PipedOutputStream();
        this.pipeIn = new PipedInputStream(pipeOut);
        this.sql = copySql;
        LOG.info("copy sql: {}", this.sql);
        changeCsvSizeLimit(connection);

        this.copyResult = new FutureTask<>(() -> {
            try {
//                CopyManager mgr = new CopyManager((BaseConnection) connection);
                CopyManager mgr = new CopyManager(connection.unwrap(BaseConnection.class));
                return mgr.copyIn(sql, pipeIn);
            }
            finally {
                try {
                    pipeIn.close();
                }
                catch (Exception ignore) {
                }
            }
        });

        copyBackendThread = new Thread(copyResult);
        copyBackendThread.setName(sql);
        copyBackendThread.setDaemon(true);
        copyBackendThread.start();
    }

    @Override
    public Long call()
            throws Exception
    {
        Thread.currentThread().setName("CopyWorker");

        byte[] data;
        try {
            while (true) {
                data = queue.poll(GPConstant.TIME_OUT_MS, TimeUnit.MILLISECONDS);

                if (data == null && !task.moreData()) {
                    break;
                }
                else if (data == null) {
                    continue;
                }

                pipeOut.write(data);
            }

            pipeOut.flush();
            pipeOut.close();
        }
        catch (Exception e) {
            try {
                connection.unwrap(BaseConnection.class).cancelQuery();
            }
            catch (SQLException ignore) {
                // ignore if failed to cancel query
            }

            try {
                copyBackendThread.interrupt();
            }
            catch (SecurityException ignore) {
            }

            try {
                copyResult.get();
            }
            catch (ExecutionException exec) {
                if (exec.getCause() instanceof PSQLException) {
                    throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, exec.getCause());
                }
                // ignore others
            }
            catch (Exception ignore) {
            }

            throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
        finally {
            try {
                pipeOut.close();
            }
            catch (Exception e) {
                // ignore if failed to close pipe
            }

            try {
                copyBackendThread.join(0);
            }
            catch (Exception e) {
                // ignore if thread is interrupted
            }

            DBUtil.closeDBResources(null, null, connection);
        }

        try {
            return copyResult.get();
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }

    private void changeCsvSizeLimit(Connection conn)
    {
        List<String> sqls = new ArrayList<>();
        sqls.add("set gp_max_csv_line_length = " + GPConstant.MAX_CSV_SIZE);

        try {
            WriterUtil.executeSqls(conn, sqls, task.getJdbcUrl(), DataBaseType.PostgreSQL);
        }
        catch (Exception e) {
            LOG.warn("Cannot set gp_max_csv_line_length to {}", GPConstant.MAX_CSV_SIZE);
        }
    }
}
