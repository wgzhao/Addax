package com.alibaba.datax.plugin.rdbms.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommonRdbmsWriter
{
    public static class Job
    {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private final DataBaseType dataBaseType;

        public Job(DataBaseType dataBaseType)
        {
            this.dataBaseType = dataBaseType;
            OriginalConfPretreatmentUtil.dataBaseType = this.dataBaseType;
        }

        public void init(Configuration originalConfig)
        {
            OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);

            LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        /*目前只支持MySQL Writer跟Oracle Writer;检查PreSQL跟PostSQL语法以及insert，delete权限*/
        public void writerPreCheck(Configuration originalConfig, DataBaseType dataBaseType)
        {
            /*检查PreSql跟PostSql语句*/
            prePostSqlValid(originalConfig, dataBaseType);
            /*检查insert 跟delete权限*/
            privilegeValid(originalConfig, dataBaseType);
        }

        public void prePostSqlValid(Configuration originalConfig, DataBaseType dataBaseType)
        {
            /*检查PreSql跟PostSql语句*/
            WriterUtil.preCheckPrePareSQL(originalConfig, dataBaseType);
            WriterUtil.preCheckPostSQL(originalConfig, dataBaseType);
        }

        public void privilegeValid(Configuration originalConfig, DataBaseType dataBaseType)
        {
            /*检查insert 跟delete权限*/
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                    Object.class);

            for (Object connection : connections) {
                Configuration connConf = Configuration.from(connection.toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
                boolean hasInsertPri = DBUtil.checkInsertPrivilege(dataBaseType, jdbcUrl, username, password, expandedTables);

                if (!hasInsertPri) {
                    throw RdbmsException.asInsertPriException(dataBaseType, originalConfig.getString(Key.USERNAME), jdbcUrl);
                }

                if (DBUtil.needCheckDeletePrivilege(originalConfig)) {
                    boolean hasDeletePri = DBUtil.checkDeletePrivilege(dataBaseType, jdbcUrl, username, password, expandedTables);
                    if (!hasDeletePri) {
                        throw RdbmsException.asDeletePriException(dataBaseType, originalConfig.getString(Key.USERNAME), jdbcUrl);
                    }
                }
            }
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        public void prepare(Configuration originalConfig)
        {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                        Object.class);
                Configuration connConf = Configuration.from(conns.get(0)
                        .toString());

                // 这里的 jdbcUrl 已经 append 了合适后缀参数
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                originalConfig.set(Key.JDBC_URL, jdbcUrl);

                String table = connConf.getList(Key.TABLE, String.class).get(0);
                originalConfig.set(Key.TABLE, table);

                List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                        String.class);
                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                        preSqls, table);

                originalConfig.remove(Constant.CONN_MARK);
                if (!renderedPreSqls.isEmpty()) {
                    // 说明有 preSql 配置，则此处删除掉
                    originalConfig.remove(Key.PRE_SQL);

                    Connection conn = DBUtil.getConnection(dataBaseType,
                            jdbcUrl, username, password);
                    LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                    WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }

            LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        public List<Configuration> split(Configuration originalConfig,
                int mandatoryNumber)
        {
            return WriterUtil.doSplit(originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        public void post(Configuration originalConfig)
        {
            int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                // 已经由 prepare 进行了appendJDBCSuffix处理
                String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

                String table = originalConfig.getString(Key.TABLE);

                List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                        String.class);
                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                        postSqls, table);

                if (!renderedPostSqls.isEmpty()) {
                    // 说明有 postSql 配置，则此处删除掉
                    originalConfig.remove(Key.POST_SQL);

                    Connection conn = DBUtil.getConnection(this.dataBaseType,
                            jdbcUrl, username, password);

                    LOG.info(
                            "Begin to execute postSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                    WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }
        }

        public void destroy(Configuration originalConfig)
        {
            //
        }
    }

    public static class Task
    {
        protected static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final String VALUE_HOLDER = "?";
        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        protected static String basicMessage;
        protected static String insertOrReplaceTemplate;
        protected DataBaseType dataBaseType;
        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected List<String> columns;
        protected List<String> preSqls;
        protected List<String> postSqls;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;
        protected String writeRecordSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

        public Task(DataBaseType dataBaseType)
        {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration writerSliceConfig)
        {
            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);

            this.table = writerSliceConfig.getString(Key.TABLE);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);

            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            insertOrReplaceTemplate = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
            this.writeRecordSql = String.format(insertOrReplaceTemplate, this.table);

            basicMessage = String.format("jdbcUrl:[%s], table:[%s]",
                    this.jdbcUrl, this.table);
        }

        public void prepare(Configuration writerSliceConfig)
        {
            Connection connection = DBUtil.getConnection(this.dataBaseType,
                    this.jdbcUrl, username, password);

            DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                    this.dataBaseType, basicMessage);

            int tableNumber = writerSliceConfig.getInt(
                    Constant.TABLE_NUMBER_MARK);
            if (tableNumber != 1) {
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                        StringUtils.join(this.preSqls, ";"), basicMessage);
                WriterUtil.executeSqls(connection, this.preSqls, basicMessage, dataBaseType);
            }

            DBUtil.closeDBResources(null, null, connection);
        }

        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection)
        {
            this.taskPluginCollector = taskPluginCollector;
            List<String> mergeColumns = new ArrayList<>();

            if (this.dataBaseType == DataBaseType.Oracle
                    && !"insert".equalsIgnoreCase(this.writeMode)) {
                LOG.info("write oracle using {} mode", this.writeMode);
                List<String> columnsOne = new ArrayList<>();
                List<String> columnsTwo = new ArrayList<>();
                String merge = this.writeMode;
                String[] sArray = WriterUtil.getStrings(merge);
                for (String s : this.columns) {
                    if (Arrays.asList(sArray).contains(s)) {
                        columnsOne.add(s);
                    }
                }
                for (String s : this.columns) {
                    if (!Arrays.asList(sArray).contains(s)) {
                        columnsTwo.add(s);
                    }
                }
                int i = 0;
                for (String column : columnsOne) {
                    mergeColumns.add(i++, column);
                }
                for (String column : columnsTwo) {
                    mergeColumns.add(i++, column);
                }
            }
            mergeColumns.addAll(this.columns);

            // 用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                    this.table, StringUtils.join(mergeColumns, ","));
            // 写数据库的SQL语句
            calcWriteRecordSql();

            List<Record> writeBuffer = new ArrayList<>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchInsert(connection, writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                }
            }
            catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
            finally {
                writeBuffer.clear();
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        // TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver,
                Configuration writerSliceConfig,
                TaskPluginCollector taskPluginCollector)
        {
            Connection connection = DBUtil.getConnection(this.dataBaseType,
                    this.jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                    this.dataBaseType, basicMessage);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connection);
        }

        public void post(Configuration writerSliceConfig)
        {
            int tableNumber = writerSliceConfig.getInt(
                    Constant.TABLE_NUMBER_MARK);

            boolean hasPostSql = (this.postSqls != null && !this.postSqls.isEmpty());
            if (tableNumber == 1 || !hasPostSql) {
                return;
            }

            Connection connection = DBUtil.getConnection(this.dataBaseType,
                    this.jdbcUrl, username, password);

            LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
                    StringUtils.join(this.postSqls, ";"), basicMessage);
            WriterUtil.executeSqls(connection, this.postSqls, basicMessage, dataBaseType);
            DBUtil.closeDBResources(null, null, connection);
        }

        public void destroy(Configuration writerSliceConfig)
        {
            //
        }

        protected void doBatchInsert(Connection connection, List<Record> buffer)
                throws SQLException
        {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);
                if (this.dataBaseType == DataBaseType.Oracle &&
                        !"insert".equalsIgnoreCase(this.writeMode)) {
                    String merge = this.writeMode;
                    String[] sArray = WriterUtil.getStrings(merge);
                    for (Record record : buffer) {
                        List<Column> recordOne = new ArrayList<>();
                        for (int j = 0; j < this.columns.size(); j++) {
                            if (Arrays.asList(sArray).contains(this.columns.get(j))) {
                                recordOne.add(record.getColumn(j));
                            }
                        }
                        for (int j = 0; j < this.columns.size(); j++) {
                            if (!Arrays.asList(sArray).contains(this.columns.get(j))) {
                                recordOne.add(record.getColumn(j));
                            }
                        }
                        for (int j = 0; j < this.columns.size(); j++) {
                            recordOne.add(record.getColumn(j));
                        }
                        for (int j = 0; j < recordOne.size(); j++) {
                            record.setColumn(j, recordOne.get(j));
                        }
                        preparedStatement = fillPreparedStatement(
                                preparedStatement, record);
                        preparedStatement.addBatch();
                    }
                }
                else {
                    for (Record record : buffer) {
                        preparedStatement = fillPreparedStatement(
                                preparedStatement, record);
                        preparedStatement.addBatch();
                    }
                }
                preparedStatement.executeBatch();
                connection.commit();
            }
            catch (SQLException e) {
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为: {}", e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            }
            catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
            finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        protected void doOneInsert(Connection connection, List<Record> buffer)
        {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(
                                preparedStatement, record);
                        preparedStatement.execute();
                    }
                    catch (SQLException e) {
                        LOG.debug(e.toString());

                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    }
                    finally {
                        // 最后不要忘了关闭 preparedStatement
                        preparedStatement.clearParameters();
                    }
                }
            }
            catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
            finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        // 直接使用了两个类变量：columnNumber,resultSetMetaData
        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
                throws SQLException
        {
            for (int i = 0; i < record.getColumnNumber(); i++) {
                int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i,
                        columnSqltype, record.getColumn(i));
            }
            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column)
                throws SQLException
        {
            java.util.Date utilDate;
            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:

                case Types.BOOLEAN:
                    preparedStatement.setString(columnIndex + 1, column
                            .asString());
                    break;

                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    }
                    else {
                        preparedStatement.setString(columnIndex + 1, strValue);
                    }
                    break;

                case Types.TINYINT:
                    Long longValue = column.asLong();
                    if (null == longValue) {
                        preparedStatement.setString(columnIndex + 1, null);
                    }
                    else {
                        preparedStatement.setString(columnIndex + 1, longValue.toString());
                    }
                    break;

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if ("year"
                            .equalsIgnoreCase(this.resultSetMetaData.getRight().get(columnIndex))) {
                        if (column.asBigInteger() == null) {
                            preparedStatement.setString(columnIndex + 1, null);
                        }
                        else {
                            preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                        }
                    }
                    else {
                        java.sql.Date sqlDate = null;
                        try {
                            utilDate = column.asDate();
                        }
                        catch (DataXException e) {
                            throw new SQLException(String.format(
                                    "Date 类型转换错误：[%s]", column));
                        }

                        if (null != utilDate) {
                            sqlDate = new java.sql.Date(utilDate.getTime());
                        }
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    }
                    catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIME 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    try {
                        utilDate = column.asDate();
                    }
                    catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIMESTAMP 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                                utilDate.getTime());
                    }
                    preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    preparedStatement.setBytes(columnIndex + 1, column
                            .asBytes());
                    break;

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (this.dataBaseType == DataBaseType.MySql) {
                        preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                    }
                    else {
                        preparedStatement.setString(columnIndex + 1, column.asString());
                    }
                    break;

                case Types.ARRAY:
                    preparedStatement.setString(columnIndex + 1, column.asString());
                    break;

                case Types.OTHER:
                    if ("image".equals(this.resultSetMetaData.getRight().get(columnIndex))) {
                        preparedStatement.setBytes(columnIndex + 1, column.asBytes());
                    }
                    else {
                        preparedStatement.setObject(columnIndex + 1, column.asString());
                    }
                    break;

                default:
                    throw DataXException
                            .asDataXException(
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

        private void calcWriteRecordSql()
        {
            if (!VALUE_HOLDER.equals(calcValueHolder(""))) {
                List<String> valueHolders = new ArrayList<>(columnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(calcValueHolder(type));
                }

                insertOrReplaceTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, dataBaseType, false);
                writeRecordSql = String.format(insertOrReplaceTemplate, this.table);
            }
        }

        protected String calcValueHolder(String columnType)
        {
            return VALUE_HOLDER;
        }
    }
}
