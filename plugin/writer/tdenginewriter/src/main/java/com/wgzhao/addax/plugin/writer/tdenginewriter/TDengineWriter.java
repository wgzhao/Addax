package com.wgzhao.addax.plugin.writer.tdenginewriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class TDengineWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.TDengine;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck()
        {
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare()
        {
            //实跑先不支持 权限 检验
//            this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE)
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post()
        {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init()
        {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE)
            {
                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqlType, Column column)
                        throws SQLException
                {
                    if (column.getRawData() == null) {
                        preparedStatement.setObject(columnIndex, null);
                        return preparedStatement;
                    }
                    switch (this.resultSetMetaData.getRight().get(columnIndex-1)) {
                        case "BOOL":
                            preparedStatement.setBoolean(columnIndex, column.asBoolean());
                            break;

                        case "SMALLINT":
                        case "TINYINT":
                        case "INT":
                        case "BIGINT":
                            preparedStatement.setLong(columnIndex, column.asLong());
                            break;

                        case "REAL":
                        case "DOUBLE":
                            preparedStatement.setDouble(columnIndex, column.asDouble());
                            break;

                        case "TIMESTAMP":
                            // TDengine timestamp min values is 1500000000000, means 2017-07-14 10:40:00.0
                            // so if timestamp less than ths min value ,it will occurred timestamp out of range
                            if (columnIndex == 1 && column.asLong() < 1500000000000L) {
                              throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR,
                                      "TDengine 能写入的时间戳最小时间为 '2017-07-14 10:40:00.0', 当前要求写入的时间为 " +
                                              Timestamp.from(Instant.ofEpochMilli(column.asLong())));
                            }
                            preparedStatement.setObject(columnIndex, column.asLong());
                            break;

                        case "BINARY":
                        case "NCHAR":
                            preparedStatement.setString(columnIndex, column.asString());
                            break;

                        default:
                            throw AddaxException
                                    .asAddaxException(
                                            DBUtilErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], " +
                                                            "字段SQL类型编号:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                                    this.resultSetMetaData.getLeft().get(columnIndex),
                                                    this.resultSetMetaData.getMiddle().get(columnIndex),
                                                    this.resultSetMetaData.getRight().get(columnIndex)));
                    }
                    return preparedStatement;
                }
            };
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.commonRdbmsWriterTask.startWrite(recordReceiver, writerSliceConfig, getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }
    }
}
