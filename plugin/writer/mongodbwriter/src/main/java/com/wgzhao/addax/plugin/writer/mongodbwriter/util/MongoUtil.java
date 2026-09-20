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

package com.wgzhao.addax.plugin.writer.mongodbwriter.util;

import com.alibaba.fastjson2.JSON;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonInvalidOperationException;
import org.bson.Document;
import org.bson.json.JsonParseException;

import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

/** Mongo Util. */
public final class MongoUtil
{

    private MongoUtil() {}

    /** Initmongoclient. */
    public static MongoClient initMongoClient(List<Object> addressList)
    {
        return initCredentialMongoClient(addressList, null, null, null);
    }

    /** Initcredentialmongoclient. */
    public static MongoClient initCredentialMongoClient(List<Object> addressList, String userName, String password, String database)
    {
        if (addressList == null || addressList.isEmpty()) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The MongoDB connection address must not be empty");
        }

        // let the driver parse the host list, it reports the reason of a malformed address itself
        String hosts = String.join(",", addressList.stream().map(String::valueOf).toList());
        MongoClientSettings.Builder builder;
        try {
            builder = MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb://" + hosts));
        }
        catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("Invalid MongoDB address [%s]: %s", hosts, e.getMessage()));
        }

        if (userName != null && !userName.isEmpty() && password != null && !password.isEmpty()) {
            // pass the password as a credential instead of embedding it into the uri,
            // which would require percent-encoding
            builder.credential(MongoCredential.createCredential(userName, database, password.toCharArray()));
        }
        return MongoClients.create(builder.build());
    }

    /**
     * Parse a filter, configured either as extended JSON text or as a json object.
     * The driver's parser is not a JavaScript engine, so a date cannot be written as the shell's
     * {@code new Date('2026-09-20')}.
     *
     * @param value the configured filter, a String of extended JSON or a parsed json object
     * @param parameter the name of the parameter, reported when the value is rejected
     * @return the filter, or null when nothing is configured
     */
    public static Document parseFilter(Object value, String parameter)
    {
        if (value == null) {
            return null;
        }
        // an object is re-serialized because only the driver's parser restores the extended JSON
        // literals, a plain nested map would send $date and $oid as ordinary field names
        String json = value instanceof String text ? text : JSON.toJSONString(value);
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return Document.parse(json);
        }
        catch (JsonParseException | BsonInvalidOperationException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, String.format(
                    "Invalid filter in the [%s] parameter: %s. The filter is read as extended JSON, not as "
                            + "JavaScript, so a date is written as {\"$date\": \"2026-09-20T00:00:00+08:00\"} "
                            + "or as {\"$date\": 1789833600000}",
                    parameter, StringUtils.removeEnd(e.getMessage(), ".")));
        }
    }
}
