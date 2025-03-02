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

package com.wgzhao.addax.plugin.reader.cassandrareader;

import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;


public class CassandraReaderHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraReaderHelper.class);
    static CodecRegistry registry = new CodecRegistry();

    static String toJSonString(Object o, DataType type)
            throws Exception
    {
        if (o == null) {
            return JSON.toJSONString(null);
        }
        switch (type.getName()) {
            case LIST:
            case MAP:
            case SET:
            case TUPLE:
            case UDT:
                return JSON.toJSONString(transferObjectForJson(o, type));

            default:
                return JSON.toJSONString(o);
        }
    }

    static Object transferObjectForJson(Object o, DataType type)
            throws TypeNotSupported
    {
        if (o == null) {
            return o;
        }
        switch (type.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BOOLEAN:
            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
            case VARINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case UUID:
            case TIMEUUID:
            case TIME:
                return o;

            case BLOB:
                ByteBuffer byteBuffer = (ByteBuffer) o;
                return Base64.encodeBase64String(
                        Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(),
                                byteBuffer.limit()));

            case DATE:
                return ((LocalDate) o).getMillisSinceEpoch();

            case TIMESTAMP:
                return ((Date) o).getTime();

            case DURATION:
                return o.toString();

            case INET:
                return ((InetAddress) o).getHostAddress();

            case LIST: {
                return transferListForJson((List) o, type.getTypeArguments().get(0));
            }

            case MAP: {
                DataType keyType = type.getTypeArguments().get(0);
                DataType valType = type.getTypeArguments().get(1);
                return transferMapForJson((Map) o, keyType, valType);
            }

            case SET: {
                return transferSetForJson((Set) o, type.getTypeArguments().get(0));
            }

            case TUPLE: {
                return transferTupleForJson((TupleValue) o, ((TupleType) type).getComponentTypes());
            }

            case UDT: {
                return transferUDTForJson((UDTValue) o);
            }

            default:
                throw new TypeNotSupported();
        }
    }

    static List transferListForJson(List clist, DataType eleType)
            throws TypeNotSupported
    {
        List result = new ArrayList();
        switch (eleType.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BOOLEAN:
            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
            case VARINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case TIME:
            case UUID:
            case TIMEUUID:
                return clist;

            case BLOB:
            case DATE:
            case TIMESTAMP:
            case DURATION:
            case INET:
            case LIST:
            case MAP:
            case SET:
            case TUPLE:
            case UDT:
                for (Object item : clist) {
                    Object newItem = transferObjectForJson(item, eleType);
                    result.add(newItem);
                }
                break;

            default:
                throw new TypeNotSupported();
        }

        return result;
    }

    static Set transferSetForJson(Set cset, DataType eleType)
            throws TypeNotSupported
    {
        Set result = new HashSet();
        switch (eleType.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BOOLEAN:
            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
            case VARINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case TIME:
            case UUID:
            case TIMEUUID:
                return cset;

            case BLOB:
            case DATE:
            case TIMESTAMP:
            case DURATION:
            case INET:
            case LIST:
            case MAP:
            case SET:
            case TUPLE:
            case UDT:
                for (Object item : cset) {
                    Object newItem = transferObjectForJson(item, eleType);
                    result.add(newItem);
                }
                break;

            default:
                throw new TypeNotSupported();
        }

        return result;
    }

    static Map<Object, Object> transferMapForJson(Map<Object, Object> cmap, DataType keyType, DataType valueType)
            throws TypeNotSupported
    {
        Map<Object, Object> newMap = new HashMap<>();
        for (Object e : cmap.entrySet()) {
            Object k = ((Map.Entry) e).getKey();
            Object v = ((Map.Entry) e).getValue();
            Object newKey = transferObjectForJson(k, keyType);
            Object newValue = transferObjectForJson(v, valueType);
            if (!(newKey instanceof String)) {
                newKey = JSON.toJSONString(newKey);
            }
            newMap.put(newKey, newValue);
        }
        return newMap;
    }

    static List<Object> transferTupleForJson(TupleValue tupleValue, List<DataType> componentTypes)
            throws TypeNotSupported
    {
        List<Object> l = new ArrayList<>();
        for (int j = 0; j < componentTypes.size(); j++) {
            DataType dataType = componentTypes.get(j);
            TypeToken<?> eltClass = registry.codecFor(dataType).getJavaType();
            Object ele = tupleValue.get(j, eltClass);
            l.add(transferObjectForJson(ele, dataType));
        }
        return l;
    }

    static Map<String, Object> transferUDTForJson(UDTValue udtValue)
            throws TypeNotSupported
    {
        Map<String, Object> newMap = new HashMap<>();
        int j = 0;
        for (UserType.Field f : udtValue.getType()) {
            DataType dataType = f.getType();
            TypeToken<?> eltClass = registry.codecFor(dataType).getJavaType();
            Object ele = udtValue.get(j, eltClass);
            newMap.put(f.getName(), transferObjectForJson(ele, dataType));
            j++;
        }
        return newMap;
    }

    static Record buildRecord(Record record, Row rs, ColumnDefinitions metaData, int columnNumber,
            TaskPluginCollector taskPluginCollector)
    {

        try {
            for (int i = 0; i < columnNumber; i++) {
                try {
                    if (rs.isNull(i)) {
                        record.addColumn(new StringColumn());
                        continue;
                    }
                    switch (metaData.getType(i).getName()) {

                        case ASCII:
                        case TEXT:
                        case VARCHAR:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case BLOB:
                            record.addColumn(new BytesColumn(rs.getBytes(i).array()));
                            break;

                        case BOOLEAN:
                            record.addColumn(new BoolColumn(rs.getBool(i)));
                            break;

                        case SMALLINT:
                            record.addColumn(new LongColumn((int) rs.getShort(i)));
                            break;

                        case TINYINT:
                            record.addColumn(new LongColumn((int) rs.getByte(i)));
                            break;

                        case INT:
                            record.addColumn(new LongColumn(rs.getInt(i)));
                            break;

                        case COUNTER:
                        case BIGINT:
                            record.addColumn(new LongColumn(rs.getLong(i)));
                            break;

                        case VARINT:
                            record.addColumn(new LongColumn(rs.getVarint(i)));
                            break;

                        case FLOAT:
                            record.addColumn(new DoubleColumn(rs.getFloat(i)));
                            break;

                        case DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getDouble(i)));
                            break;

                        case DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getDecimal(i)));
                            break;

                        case DATE:
                            record.addColumn(new DateColumn(rs.getDate(i).getMillisSinceEpoch()));
                            break;

                        case TIME:
                            record.addColumn(new LongColumn(rs.getTime(i)));
                            break;

                        case TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case UUID:
                        case TIMEUUID:
                            record.addColumn(new StringColumn(rs.getUUID(i).toString()));
                            break;

                        case INET:
                            record.addColumn(new StringColumn(rs.getInet(i).getHostAddress()));
                            break;

                        case DURATION:
                            record.addColumn(new StringColumn(rs.get(i, Duration.class).toString()));
                            break;

                        case LIST: {
                            TypeToken listEltClass = registry.codecFor(metaData.getType(i).getTypeArguments().get(0)).getJavaType();
                            List<?> l = rs.getList(i, listEltClass);
                            record.addColumn(new StringColumn(toJSonString(l, metaData.getType(i))));
                        }
                        break;

                        case MAP: {
                            DataType keyType = metaData.getType(i).getTypeArguments().get(0);
                            DataType valType = metaData.getType(i).getTypeArguments().get(1);
                            TypeToken<?> keyEltClass = registry.codecFor(keyType).getJavaType();
                            TypeToken<?> valEltClass = registry.codecFor(valType).getJavaType();
                            Map<?, ?> m = rs.getMap(i, keyEltClass, valEltClass);
                            record.addColumn(new StringColumn(toJSonString(m, metaData.getType(i))));
                        }
                        break;

                        case SET: {
                            TypeToken<?> setEltClass = registry.codecFor(metaData.getType(i).getTypeArguments().get(0))
                                    .getJavaType();
                            Set<?> set = rs.getSet(i, setEltClass);
                            record.addColumn(new StringColumn(toJSonString(set, metaData.getType(i))));
                        }
                        break;

                        case TUPLE: {
                            TupleValue t = rs.getTupleValue(i);
                            record.addColumn(new StringColumn(toJSonString(t, metaData.getType(i))));
                        }
                        break;

                        case UDT: {
                            UDTValue t = rs.getUDTValue(i);
                            record.addColumn(new StringColumn(toJSonString(t, metaData.getType(i))));
                        }
                        break;

                        default:
                            throw AddaxException
                                    .asAddaxException(
                                            CONFIG_ERROR,
                                            "The column type is not supported. column name: " + metaData.getName(i)
                                                    + ", column type: " + metaData.getType(i));
                    }
                }
                catch (TypeNotSupported t) {
                    throw AddaxException
                            .asAddaxException(
                                    CONFIG_ERROR,
                                    "The column type is not supported. column name: " + metaData.getName(i)
                                            + ", column type: " + metaData.getType(i));
                }
            }
        }
        catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, e);
            if (e instanceof AddaxException) {
                throw (AddaxException) e;
            }
            return null;
        }
        return record;
    }

    public static List<Configuration> splitJob(int adviceNumber, Configuration jobConfig, Cluster cluster)
    {
        List<Configuration> splitConfigs = new ArrayList<Configuration>();
        if (adviceNumber <= 1) {
            splitConfigs.add(jobConfig);
            return splitConfigs;
        }
        String where = jobConfig.getString(MyKey.WHERE);
        if (where != null && where.toLowerCase().contains("token(")) {
            splitConfigs.add(jobConfig);
            return splitConfigs;
        }
        String partitioner = cluster.getMetadata().getPartitioner();
        if (partitioner.endsWith("RandomPartitioner")) {
            BigDecimal minToken = BigDecimal.valueOf(-1);
            BigDecimal maxToken = new BigDecimal(new BigInteger("2").pow(127));
            BigDecimal step = maxToken.subtract(minToken)
                    .divide(BigDecimal.valueOf(adviceNumber), 2, RoundingMode.HALF_EVEN);
            for (int i = 0; i < adviceNumber; i++) {
                BigInteger l = minToken.add(step.multiply(BigDecimal.valueOf(i))).toBigInteger();
                BigInteger r = minToken.add(step.multiply(BigDecimal.valueOf(i + 1))).toBigInteger();
                if (i == adviceNumber - 1) {
                    r = maxToken.toBigInteger();
                }
                Configuration taskConfig = jobConfig.clone();
                taskConfig.set(MyKey.MIN_TOKEN, l.toString());
                taskConfig.set(MyKey.MAX_TOKEN, r.toString());
                splitConfigs.add(taskConfig);
            }
        }
        else if (partitioner.endsWith("Murmur3Partitioner")) {
            BigDecimal minToken = BigDecimal.valueOf(Long.MIN_VALUE);
            BigDecimal maxToken = BigDecimal.valueOf(Long.MAX_VALUE);
            BigDecimal step = maxToken.subtract(minToken)
                    .divide(BigDecimal.valueOf(adviceNumber), 2, RoundingMode.HALF_EVEN);
            for (int i = 0; i < adviceNumber; i++) {
                long l = minToken.add(step.multiply(BigDecimal.valueOf(i))).longValue();
                long r = minToken.add(step.multiply(BigDecimal.valueOf(i + 1))).longValue();
                if (i == adviceNumber - 1) {
                    r = maxToken.longValue();
                }
                Configuration taskConfig = jobConfig.clone();
                taskConfig.set(MyKey.MIN_TOKEN, String.valueOf(l));
                taskConfig.set(MyKey.MAX_TOKEN, String.valueOf(r));
                splitConfigs.add(taskConfig);
            }
        }
        else {
            splitConfigs.add(jobConfig);
        }
        return splitConfigs;
    }

    public static String getQueryString(Configuration taskConfig, Cluster cluster)
    {
        List<String> columnMeta = taskConfig.getList(MyKey.COLUMN, String.class);
        String keyspace = taskConfig.getString(MyKey.KEYSPACE);
        String table = taskConfig.getString(MyKey.TABLE);

        StringBuilder columns = new StringBuilder();
        for (String column : columnMeta) {
            if (columns.length() > 0) {
                columns.append(",");
            }
            columns.append(column);
        }

        StringBuilder where = new StringBuilder();
        String whereString = taskConfig.getString(MyKey.WHERE);
        if (whereString != null && !whereString.isEmpty()) {
            where.append(whereString);
        }
        String minToken = taskConfig.getString(MyKey.MIN_TOKEN);
        String maxToken = taskConfig.getString(MyKey.MAX_TOKEN);
        if (minToken != null || maxToken != null) {
            LOG.info("range:" + minToken + "~" + maxToken);
            List<ColumnMetadata> pks = cluster.getMetadata().getKeyspace(keyspace).getTable(table).getPartitionKey();
            StringBuilder sb = new StringBuilder();
            for (ColumnMetadata pk : pks) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(pk.getName());
            }
            String s = sb.toString();
            if (minToken != null && !minToken.isEmpty()) {
                if (where.length() > 0) {
                    where.append(" AND ");
                }
                where.append("token(").append(s).append(")").append(" > ").append(minToken);
            }
            if (maxToken != null && !maxToken.isEmpty()) {
                if (where.length() > 0) {
                    where.append(" AND ");
                }
                where.append("token(").append(s).append(")").append(" <= ").append(maxToken);
            }
        }

        boolean allowFiltering = taskConfig.getBool(MyKey.ALLOW_FILTERING, false);

        StringBuilder select = new StringBuilder();
        select.append("SELECT ").append(columns).append(" FROM ").append(table);
        if (where.length() > 0) {
            select.append(" where ").append(where);
        }
        if (allowFiltering) {
            select.append(" ALLOW FILTERING");
        }
        select.append(";");
        return select.toString();
    }

    public static void checkConfig(Configuration jobConfig, Cluster cluster)
    {
        ensureStringExists(jobConfig, MyKey.HOST);
        ensureStringExists(jobConfig, MyKey.KEYSPACE);
        ensureStringExists(jobConfig, MyKey.TABLE);
        ensureExists(jobConfig, MyKey.COLUMN);

        ///keyspace,table is exists or not
        String keyspace = jobConfig.getString(MyKey.KEYSPACE);
        if (cluster.getMetadata().getKeyspace(keyspace) == null) {
            throw AddaxException
                    .asAddaxException(
                            CONFIG_ERROR,
                            "The keyspace '" + keyspace + "' does not exist.");
        }
        String table = jobConfig.getString(MyKey.TABLE);
        TableMetadata tableMetadata = cluster.getMetadata().getKeyspace(keyspace).getTable(table);
        if (tableMetadata == null) {
            throw AddaxException
                    .asAddaxException(
                            CONFIG_ERROR,
                            "The table '" + table + "' does not exist.");
        }
        List<String> columns = jobConfig.getList(MyKey.COLUMN, String.class);
        for (String name : columns) {
            if (name == null || name.isEmpty()) {
                throw AddaxException
                        .asAddaxException(
                                CONFIG_ERROR,
                                "The column must include '" + MyKey.COLUMN_NAME + "' field.");
            }
        }
    }

    static void ensureExists(Configuration jobConfig, String keyword)
    {
        if (jobConfig.get(keyword) == null) {
            throw AddaxException
                    .asAddaxException(
                            CONFIG_ERROR,
                            "The configuration item '" + keyword + "' is required.");
        }
    }

    static void ensureStringExists(Configuration jobConfig, String keyword)
    {
        ensureExists(jobConfig, keyword);
        if (jobConfig.getString(keyword).isEmpty()) {
            throw AddaxException
                    .asAddaxException(
                            CONFIG_ERROR,
                            "The configuration item '" + keyword + "' is not empty.");
        }
    }

    static class TypeNotSupported
            extends Exception {}
}
