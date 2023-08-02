package com.wgzhao.addax.plugin.writer.databendwriter.util;

import com.alibaba.druid.sql.parser.ParserException;
import com.google.common.base.Strings;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.writer.databendwriter.DatabendWriterOptions;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public final class DatabendWriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterUtil.class);

    public final static String ONCONFLICT_COLUMN = "onConflictColumn";

    private DatabendWriterUtil() {
    }

    public static void dealWriteMode(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        List<String> onConflictColumns = originalConfig.getList(ONCONFLICT_COLUMN, String.class);
        StringBuilder writeDataSqlTemplate = new StringBuilder();

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Key.CONNECTION, Key.JDBC_URL));

        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");
        LOG.info("write mode is {}", writeMode);
        if (writeMode.toLowerCase().contains("replace")) {
            if (onConflictColumns == null || onConflictColumns.size() == 0) {
                LOG.error("Replace mode must has onConflictColumn conf");
                return;
            }
            // for databend if you want to use replace mode, the writeMode should be:  "writeMode": "replace"
            writeDataSqlTemplate.append("REPLACE INTO %s (")
                    .append(StringUtils.join(columns, ",")).append(") ").append(onConFlictDoString(onConflictColumns))
                    .append(" VALUES");

            LOG.info("Replace data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);
            originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
        } else {
            writeDataSqlTemplate.append("INSERT INTO %s");
            StringJoiner columnString = new StringJoiner(",");

            for (String column : columns) {
                columnString.add(column);
            }
            writeDataSqlTemplate.append(String.format("(%s)", columnString));
            writeDataSqlTemplate.append(" VALUES");

            LOG.info("Insert data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

            originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
        }
    }

    public static String onConFlictDoString(List<String> conflictColumns) {
        return " ON " +
                "(" +
                StringUtils.join(conflictColumns, ",") + ") ";
    }
}