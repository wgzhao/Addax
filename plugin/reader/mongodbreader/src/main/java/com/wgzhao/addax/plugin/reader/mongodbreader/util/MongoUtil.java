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

package com.wgzhao.addax.plugin.reader.mongodbreader.util;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.wgzhao.addax.common.exception.AddaxException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.CommonErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.RUNTIME_ERROR;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 * Modified by mingyan.zc on 2016/6/13.
 */
public class MongoUtil
{

    private MongoUtil() {}

    public static MongoClient initMongoClient(List<Object> addressList)
    {
        return initCredentialMongoClient(addressList, "", "", null);
    }

    public static MongoClient initCredentialMongoClient(List<Object> addressList, String userName, String password, String database)
    {

        if (!isHostPortPattern(addressList)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "不合法参数");
        }
        try {
            MongoCredential credential = null;
            if (! userName.isEmpty() && ! password.isEmpty()) {
                credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            }
            MongoClientSettings.Builder mongoBuilder = MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> {
                        try {
                            builder.hosts(parseServerAddress(addressList));
                        }
                        catch (UnknownHostException e) {
                            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "不合法的地址");
                        }
                    });
            if (credential != null) {
                mongoBuilder.credential(credential);
            }
            return MongoClients.create(mongoBuilder.build());

        }
        catch (NumberFormatException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "不合法参数");
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, "未知异常");
        }
    }

    /**
     * 判断地址类型是否符合要求
     *
     * @param addressList host list
     * @return boolean
     */
    private static boolean isHostPortPattern(List<Object> addressList)
    {
        for (Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if (!((String) address).matches(regex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 转换为mongo地址协议
     *
     * @param rawAddressList raw address list
     * @return List of ServerAddress
     * @throws UnknownHostException can not find host or ip address
     */
    private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList)
            throws UnknownHostException
    {
        List<ServerAddress> addressList = new ArrayList<>();
        for (Object address : rawAddressList) {
            String[] tempAddress = ((String) address).split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0], Integer.parseInt(tempAddress[1]));
                addressList.add(sa);
            }
            catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }
}
