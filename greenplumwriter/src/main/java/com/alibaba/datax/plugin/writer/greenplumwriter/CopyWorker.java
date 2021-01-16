package com.alibaba.datax.plugin.writer.greenplumwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
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
        changeCsvSizelimit(connection);

        this.copyResult = new FutureTask<>(() -> {
            try {
                CopyManager mgr = new CopyManager((BaseConnection) connection);
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
                data = queue.poll(Constant.TIME_OUT_MS, TimeUnit.MILLISECONDS);

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
                ((BaseConnection) connection).cancelQuery();
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
                    throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, exec.getCause());
                }
                // ignore others
            }
            catch (Exception ignore) {
            }

            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
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
            throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }

    private void changeCsvSizelimit(Connection conn)
    {
        List<String> sqls = new ArrayList<>();
        sqls.add("set gp_max_csv_line_length = " + Constant.MAX_CSV_SIZE);

        try {
            WriterUtil.executeSqls(conn, sqls, task.getJdbcUrl(), DataBaseType.PostgreSQL);
        }
        catch (Exception e) {
            LOG.warn("Cannot set gp_max_csv_line_length to {}", Constant.MAX_CSV_SIZE);
        }
    }
}
