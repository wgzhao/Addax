package com.wgzhao.addax.plugin.writer.databendwriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.writer.databendwriter.util.DatabendWriterUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class DatabendWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Databend;

    public static class Job
            extends Writer.Job {
        private Configuration originalConfig;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
            // placeholder currently not supported by databend driver, needs special treatment
            DatabendWriterUtil.dealWriteMode(this.originalConfig);
        }

        @Override
        public void preCheck() {
            this.init();
            this.commonRdbmsWriterMaster.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }
    }


    public static class Task extends Writer.Task {

        private Configuration writerSliceConfig;

        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();

            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DataBaseType.Databend)
            {
                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqlType, Column column)
                        throws SQLException {
                    switch (columnSqlType) {
                        case Types.TINYINT:
                        case Types.SMALLINT:
                        case Types.INTEGER:
                            preparedStatement.setInt(columnIndex, column.asBigInteger().intValue());
                            return preparedStatement;
                        case Types.BIGINT:
                            preparedStatement.setLong(columnIndex, column.asLong());
                            return preparedStatement;
                        case Types.JAVA_OBJECT:
                            // cast variant / array into string is fine.
                            preparedStatement.setString(columnIndex, column.asString());
                            return preparedStatement;
                        case Types.BLOB:
                        case Types.BINARY:
                            preparedStatement.setString(columnIndex,bytesToHex(column.asBytes()));
                            return preparedStatement;
                    }
                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }
            };
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.commonRdbmsWriterSlave.startWrite(lineReceiver, this.writerSliceConfig, this.getTaskPluginCollector());
        }

        private String bytesToHex(byte[] bytes) {
            StringBuilder hexString = new StringBuilder();
            for (byte b : bytes) {
                String hex = Integer.toHexString(0xFF & b);
                if (hex.length() == 1) {
                    hexString.append('0'); // Pad with leading zero if necessary
                }
                hexString.append(hex);
            }
            return hexString.toString();
        }
    }
}