/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.writer.paimonwriter;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;

public class PaimonHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonHelper.class);

    public static void kerberosAuthentication(org.apache.hadoop.conf.Configuration hadoopConf, String kerberosPrincipal, String kerberosKeytabFilePath) throws Exception {
        if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos authentication failed, keytab file: [%s], principal: [%s]",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw AddaxException.asAddaxException(LOGIN_ERROR, e);
            }
        }
    }

    public static Options getOptions(Configuration conf){
        Options options = new Options();
        conf.getMap("paimonConfig").forEach((k, v) -> options.set(k, String.valueOf(v)));
        return options;
    }

    public static CatalogContext getCatalogContext(Options options) {
        CatalogContext context = null;
        String warehouse=options.get("warehouse");
        if (warehouse ==null || warehouse.isEmpty()){
            throw new RuntimeException("warehouse of the paimonConfig is null");
        }
        if(warehouse.startsWith("hdfs://")||warehouse.startsWith("s3a://")){

            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            options.toMap().forEach((k, v) -> hadoopConf.set(k, String.valueOf(v)));
            UserGroupInformation.setConfiguration(hadoopConf);
            context = CatalogContext.create(options,hadoopConf);
        } else {

            context = CatalogContext.create(options);
        }

        return context;
    }
}
