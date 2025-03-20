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

package com.wgzhao.addax.plugin.writer.cassandrawriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.exception.AddaxException;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import org.apache.commons.codec.binary.Base64;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

/**
 * Created by mazhenlin on 2019/8/21.
 */
public class CassandraWriterHelper
{
    static CodecRegistry registry = new CodecRegistry();

    public static Object parseFromString(String s, DataType sqlType)
            throws Exception
    {
        if (s == null || s.isEmpty()) {
            if (sqlType.getName() == Name.ASCII || sqlType.getName() == Name.TEXT ||
                    sqlType.getName() == Name.VARCHAR) {
                return s;
            }
            else {
                return null;
            }
        }
        switch (sqlType.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return s;

            case BLOB:
                byte[] byteArray = new byte[s.length() / 2];
                for (int i = 0; i < byteArray.length; i++) {
                    String subStr = s.substring(2 * i, 2 * i + 2);
                    byteArray[i] = ((byte) Integer.parseInt(subStr, 16));
                }
                return ByteBuffer.wrap(byteArray);

            case BOOLEAN:
                return Boolean.valueOf(s);

            case TINYINT:
                return Byte.valueOf(s);

            case SMALLINT:
                return Short.valueOf(s);

            case INT:
                return Integer.valueOf(s);

            case BIGINT:

            case TIME:
                return Long.valueOf(s);

            case VARINT:
                return new BigInteger(s, 10);

            case FLOAT:
                return Float.valueOf(s);

            case DOUBLE:
                return Double.valueOf(s);

            case DECIMAL:
                return new BigDecimal(s);

            case DATE: {
                String[] a = s.split("-");
                if (a.length != 3) {
                    throw new Exception("The date format is not correct, it must be yyyy-mm-dd");
                }
                return LocalDate.fromYearMonthDay(Integer.parseInt(a[0]), Integer.parseInt(a[1]),
                        Integer.parseInt(a[2]));
            }

            case TIMESTAMP:
                return new Date(Long.parseLong(s));

            case UUID:
            case TIMEUUID:
                return UUID.fromString(s);

            case INET:
                String[] b = s.split("/");
                if (b.length < 2) {
                    return InetAddress.getByName(s);
                }
                byte[] address = InetAddress.getByName(b[1]).getAddress();
                return InetAddress.getByAddress(b[0], address);

            case DURATION:
                return Duration.from(s);

            case LIST:
            case MAP:
            case SET:
            case TUPLE:
            case UDT:
                Object jsonObject = JSON.parse(s);
                return parseFromJson(jsonObject, sqlType);

            default:
                throw AddaxException.asAddaxException(CONFIG_ERROR, "The data type is not supported: " + sqlType);
        } // end switch
    }

    public static Object parseFromJson(Object jsonObject, DataType type)
            throws Exception
    {
        if (jsonObject == null) {
            return null;
        }
        switch (type.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BOOLEAN:
            case TIME:
                return jsonObject;

            case TINYINT:
                return ((Number) jsonObject).byteValue();

            case SMALLINT:
                return ((Number) jsonObject).shortValue();

            case INT:
                return ((Number) jsonObject).intValue();

            case BIGINT:
                return ((Number) jsonObject).longValue();

            case VARINT:
                return new BigInteger(jsonObject.toString());

            case FLOAT:
                return ((Number) jsonObject).floatValue();

            case DOUBLE:
                return ((Number) jsonObject).doubleValue();

            case DECIMAL:
                return new BigDecimal(jsonObject.toString());

            case BLOB:
                return ByteBuffer.wrap(Base64.decodeBase64((String) jsonObject));

            case DATE:
                return LocalDate.fromMillisSinceEpoch(((Number) jsonObject).longValue());

            case TIMESTAMP:
                return new Date(((Number) jsonObject).longValue());

            case DURATION:
                return Duration.from(jsonObject.toString());

            case UUID:
            case TIMEUUID:
                return UUID.fromString(jsonObject.toString());

            case INET:
                return InetAddress.getByName((String) jsonObject);

            case LIST:
                List<Object> l = new ArrayList<>();
                for (Object o : (JSONArray) jsonObject) {
                    l.add(parseFromJson(o, type.getTypeArguments().get(0)));
                }
                return l;

            case MAP: {
                Map<Object, Object> m = new HashMap<>();
                for (Map.Entry<String, Object> e : ((JSONObject) jsonObject).entrySet()) {
                    Object k = parseFromString(e.getKey(), type.getTypeArguments().get(0));
                    Object v = parseFromJson(e.getValue(), type.getTypeArguments().get(1));
                    m.put(k, v);
                }
                return m;
            }

            case SET:
                Set<Object> s = new HashSet<>();
                for (Object o : (JSONArray) jsonObject) {
                    s.add(parseFromJson(o, type.getTypeArguments().get(0)));
                }
                return s;

            case TUPLE: {
                TupleValue t = ((TupleType) type).newValue();
                int j = 0;
                for (Object e : (JSONArray) jsonObject) {
                    DataType eleType = ((TupleType) type).getComponentTypes().get(j);
                    t.set(j, parseFromJson(e, eleType), registry.codecFor(eleType).getJavaType());
                    j++;
                }
                return t;
            }

            case UDT: {
                UDTValue t = ((UserType) type).newValue();
                UserType userType = t.getType();
                for (Map.Entry<String, Object> e : ((JSONObject) jsonObject).entrySet()) {
                    DataType eleType = userType.getFieldType(e.getKey());
                    t.set(e.getKey(), parseFromJson(e.getValue(), eleType), registry.codecFor(eleType).getJavaType());
                }
                return t;
            }
            default:
                return null;
        }
    }

    public static void setupColumn(BoundStatement ps, int pos, DataType sqlType, Column col)
            throws Exception
    {
        if (col.getRawData() != null) {
            switch (sqlType.getName()) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    ps.setString(pos, col.asString());
                    break;

                case BLOB:
                    ps.setBytes(pos, ByteBuffer.wrap(col.asBytes()));
                    break;

                case BOOLEAN:
                    ps.setBool(pos, col.asBoolean());
                    break;

                case TINYINT:
                    ps.setByte(pos, col.asLong().byteValue());
                    break;

                case SMALLINT:
                    ps.setShort(pos, col.asLong().shortValue());
                    break;

                case INT:
                    ps.setInt(pos, col.asLong().intValue());
                    break;

                case BIGINT:
                    ps.setLong(pos, col.asLong());
                    break;

                case VARINT:
                    ps.setVarint(pos, col.asBigInteger());
                    break;

                case FLOAT:
                    ps.setFloat(pos, col.asDouble().floatValue());
                    break;

                case DOUBLE:
                    ps.setDouble(pos, col.asDouble());
                    break;

                case DECIMAL:
                    ps.setDecimal(pos, col.asBigDecimal());
                    break;

                case DATE:
                    ps.setDate(pos, LocalDate.fromMillisSinceEpoch(col.asDate().getTime()));
                    break;

                case TIME:
                    ps.setTime(pos, col.asLong());
                    break;

                case TIMESTAMP:
                    ps.setTimestamp(pos, col.asDate());
                    break;

                case UUID:
                case TIMEUUID:
                    ps.setUUID(pos, UUID.fromString(col.asString()));
                    break;

                case INET:
                    ps.setInet(pos, InetAddress.getByName(col.asString()));
                    break;

                case DURATION:
                    ps.set(pos, Duration.from(col.asString()), Duration.class);
                    break;

                case LIST:
                    ps.setList(pos, (List<?>) parseFromString(col.asString(), sqlType));
                    break;

                case MAP:
                    ps.setMap(pos, (Map<?, ?>) parseFromString(col.asString(), sqlType));
                    break;

                case SET:
                    ps.setSet(pos, (Set<?>) parseFromString(col.asString(), sqlType));
                    break;

                case TUPLE:
                    ps.setTupleValue(pos, (TupleValue) parseFromString(col.asString(), sqlType));
                    break;

                case UDT:
                    ps.setUDTValue(pos, (UDTValue) parseFromString(col.asString(), sqlType));
                    break;

                default:
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "The data type is not supported: " + sqlType);
            } // end switch
        }
        else {
            ps.setToNull(pos);
        }
    }
}
