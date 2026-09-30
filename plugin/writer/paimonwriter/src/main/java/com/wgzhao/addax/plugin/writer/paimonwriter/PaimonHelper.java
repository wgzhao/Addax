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
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;

/** Paimon Helper. */
public class PaimonHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonHelper.class);

    /** Kerberosauthentication. */
    public static void kerberosAuthentication(org.apache.hadoop.conf.Configuration hadoopConf, String kerberosPrincipal, String kerberosKeytabFilePath) throws Exception {
        if (StringUtils.isBlank(kerberosPrincipal) || StringUtils.isBlank(kerberosKeytabFilePath)) {
            // the caller only gets here when hadoop.security.authentication says kerberos,
            // logging in needs both, and skipping it silently would fail later with a
            // message that points nowhere near the missing parameter
            throw AddaxException.asAddaxException(CONFIG_ERROR, String.format(
                    "kerberos authentication is configured but %s is missing",
                    StringUtils.isBlank(kerberosPrincipal) ? "hadoop.kerberos.principal" : "hadoop.kerberos.keytab"));
        }
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

    /** Kerberos is requested by naming it as the authentication of the Paimon catalog. */
    public static boolean isKerberos(Options options) {
        return "kerberos".equalsIgnoreCase(options.get("hadoop.security.authentication"));
    }

    /** Returns the options. */
    public static Options getOptions(Configuration conf){
        Map<String, Object> paimonConfig = conf.getMap("paimonConfig");
        if (paimonConfig == null) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "paimonConfig is required");
        }
        Options options = new Options();
        paimonConfig.forEach((k, v) -> options.set(k, String.valueOf(v)));
        return options;
    }

    /** Returns the catalogcontext. */
    public static CatalogContext getCatalogContext(Options options) {
        CatalogContext context = null;
        String warehouse=options.get("warehouse");
        if (warehouse ==null || warehouse.isEmpty()){
            throw AddaxException.asAddaxException(CONFIG_ERROR, "warehouse of the paimonConfig is null");
        }
        if (needsHadoopConf(warehouse)) {
            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            options.toMap().forEach((k, v) -> hadoopConf.set(k, String.valueOf(v)));
            UserGroupInformation.setConfiguration(hadoopConf);
            context = CatalogContext.create(options,hadoopConf);
        } else {

            context = CatalogContext.create(options);
        }

        return context;
    }

    /**
     * A warehouse that names anything but the local file system is reached through Hadoop,
     * so it needs the Hadoop configuration (and the credentials) built from the options.
     */
    private static boolean needsHadoopConf(String warehouse) {
        int colon = warehouse.indexOf(':');
        if (colon <= 0) {
            // no scheme at all, a plain path is local
            return false;
        }
        return !"file".equalsIgnoreCase(warehouse.substring(0, colon));
    }

    /**
     * Rejects a table layout an offline writer cannot assign a bucket for.
     * <p>
     * In dynamic bucket mode ('bucket' = '-1', which is the default of a primary key table)
     * the bucket of a key belongs to Paimon's own assigner, and an offline batch writer takes
     * no part in it. Assigning a bucket here scatters the rows of one primary key over several
     * buckets, and Paimon merges keys only inside a bucket, so the table ends up with
     * duplicate primary keys.
     */
    public static void validateBucketMode(Table table) {
        int bucket = new CoreOptions(table.options()).bucket();
        if (bucket == -1 && !table.primaryKeys().isEmpty()) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, String.format(
                    "the table [%s] uses dynamic bucket mode ('bucket' = '-1'), which an offline writer cannot "
                            + "write: the bucket of a key is maintained by Paimon's assigner. Recreate the table "
                            + "with a fixed bucket number (for example 'bucket' = '32'), or with postpone bucket "
                            + "mode ('bucket' = '-2', the rows stay in bucket-postpone until Paimon compacts them "
                            + "into regular buckets).", table.name()));
        }
    }
}
