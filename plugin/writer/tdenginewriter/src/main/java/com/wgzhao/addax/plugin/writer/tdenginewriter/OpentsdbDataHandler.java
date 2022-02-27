/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.writer.tdenginewriter;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class OpentsdbDataHandler
        implements DataHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(OpentsdbDataHandler.class);
    private SchemalessWriter writer;

    private String jdbcUrl;
    private String user;
    private String password;
    int batchSize;

    public OpentsdbDataHandler(Configuration config)
    {
        // opentsdb json protocol use JNI and schemaless API to write
        this.jdbcUrl = config.getString(Key.JDBC_URL);
        this.user = config.getString(Key.USERNAME, "root");
        this.password = config.getString(Key.PASSWORD, "taosdata");
        this.batchSize = config.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
    }

    @Override
    public int handle(RecordReceiver lineReceiver, TaskPluginCollector collector)
    {
        int count = 0;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);) {
            LOG.info("connection[ jdbcUrl: " + jdbcUrl + ", username: " + user + "] established.");
            writer = new SchemalessWriter(conn);
            count = write(lineReceiver, batchSize);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e);
        }

        return count;
    }

    private int write(RecordReceiver lineReceiver, int batchSize)
            throws AddaxException
    {
        int recordIndex = 1;
        try {
            Record record;
            StringBuilder sb = new StringBuilder();
            while ((record = lineReceiver.getFromReader()) != null) {
                if (batchSize == 1) {
                    String jsonData = recordToString(record);
                    LOG.debug(">>> " + jsonData);
                    writer.write(jsonData, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
                }
                else if (recordIndex % batchSize == 1) {
                    sb.append("[").append(recordToString(record)).append(",");
                }
                else if (recordIndex % batchSize == 0) {
                    sb.append(recordToString(record)).append("]");
                    String jsonData = sb.toString();
                    LOG.debug(">>> " + jsonData);
                    writer.write(jsonData, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
                    sb.delete(0, sb.length());
                }
                else {
                    sb.append(recordToString(record)).append(",");
                }
                recordIndex++;
            }
            if (sb.length() != 0 && sb.charAt(0) == '[') {
                String jsonData = sb.deleteCharAt(sb.length() - 1).append("]").toString();
                System.err.println(jsonData);
                LOG.debug(">>> " + jsonData);
                writer.write(jsonData, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(TDengineWriterErrorCode.RUNTIME_EXCEPTION, e);
        }
        return recordIndex - 1;
    }

    private String recordToString(Record record)
    {
        int recordLength = record.getColumnNumber();
        if (0 == recordLength) {
            return "";
        }
        Column column;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < recordLength; i++) {
            column = record.getColumn(i);
            sb.append(column.asString()).append("\t");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
