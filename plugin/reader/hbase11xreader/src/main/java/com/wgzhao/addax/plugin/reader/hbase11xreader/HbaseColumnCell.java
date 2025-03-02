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

package com.wgzhao.addax.plugin.reader.hbase11xreader;

import com.wgzhao.addax.common.base.BaseObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseColumnCell
        extends BaseObject
{
    private final ColumnType columnType;
    // it configured true when columnValue is not null
    private final boolean isConstant;
    // cf:qualifier
    private String columnName;
    private byte[] columnFamily;
    private byte[] qualifier;
    private String columnValue;
    private String dateFormat;

    private HbaseColumnCell(Builder builder)
    {
        this.columnType = builder.columnType;

        Validate.isTrue(builder.columnName == null || builder.columnValue == null, "you can not both setup column name and column value.");

        //columnName 和 columnValue 不能都为 null
        Validate.isTrue(builder.columnName != null || builder.columnValue != null, "you must setup one of column name and column value.");

        if (builder.columnName != null) {
            this.isConstant = false;
            this.columnName = builder.columnName;
            if (!Hbase11xHelper.isRowkeyColumn(this.columnName)) {
                String[] cfAndQualifier = this.columnName.split(":");
                Validate.isTrue(cfAndQualifier.length == 2 && StringUtils.isNotBlank(cfAndQualifier[0]) && StringUtils.isNotBlank(cfAndQualifier[1]),
                        "The column name must be formed as cf:qualifier");

                this.columnFamily = Bytes.toBytes(cfAndQualifier[0].trim());
                this.qualifier = Bytes.toBytes(cfAndQualifier[1].trim());
            }
        }
        else {
            this.isConstant = true;
            this.columnValue = builder.columnValue;
        }

        if (builder.dateFormat != null) {
            this.dateFormat = builder.dateFormat;
        }
    }

    public ColumnType getColumnType()
    {
        return columnType;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public byte[] getColumnFamily()
    {
        return columnFamily;
    }

    public byte[] getQualifier()
    {
        return qualifier;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }

    public String getColumnValue()
    {
        return columnValue;
    }

    public boolean isConstant()
    {
        return isConstant;
    }

    public static class Builder
    {
        private final ColumnType columnType;
        private String columnName;
        private String columnValue;

        private String dateFormat;

        public Builder(ColumnType columnType)
        {
            this.columnType = columnType;
        }

        public Builder columnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public Builder columnValue(String columnValue)
        {
            this.columnValue = columnValue;
            return this;
        }

        public Builder dateFormat(String dateFormat)
        {
            this.dateFormat = dateFormat;
            return this;
        }

        public HbaseColumnCell build()
        {
            return new HbaseColumnCell(this);
        }
    }
}
