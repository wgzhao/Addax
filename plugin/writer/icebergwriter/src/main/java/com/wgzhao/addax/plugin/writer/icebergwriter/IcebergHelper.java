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

package com.wgzhao.addax.plugin.writer.icebergwriter;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.wgzhao.addax.core.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.core.base.Key.KERBEROS_PRINCIPAL;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;

/** Iceberg Helper. */
public class IcebergHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(IcebergHelper.class);

    /** Kerberosauthentication. */
    public static void kerberosAuthentication(org.apache.hadoop.conf.Configuration hadoopConf, String kerberosPrincipal, String kerberosKeytabFilePath)
            throws Exception
    {
        if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            }
            catch (Exception e) {
                String message = String.format("kerberos authentication failed, keytab file: [%s], principal: [%s]",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw AddaxException.asAddaxException(LOGIN_ERROR, e);
            }
        }
    }

    /** Returns the table name, the reader and the writer share it. */
    public static String getTableName(Configuration conf)
    {
        String tableName = conf.getString("tableName");
        if (StringUtils.isBlank(tableName)) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "tableName is not set");
        }
        return tableName.trim();
    }

    /** Closes the catalog, whatever the catalog type is. */
    public static void closeCatalog(Catalog catalog)
    {
        // the catalog interface itself has no close, only its implementations do
        if (catalog instanceof Closeable closeable) {
            try {
                closeable.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Returns the catalog. */
    public static Catalog getCatalog(Configuration conf)
            throws Exception
    {
        String catalogType = conf.getString("catalogType");
        if (StringUtils.isBlank(catalogType)) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "catalogType is not set");
        }
        catalogType = catalogType.trim();

        String warehouse = StringUtils.trimToNull(conf.getString("warehouse"));
        if (warehouse == null) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "warehouse is not set");
        }

        // the default configuration reads core-site.xml from the classpath, which is what the job
        // expects when it carries no hadoop settings of its own. Handing null to the catalogs used to
        // fail while they resolved the warehouse
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        if (conf.getConfiguration("hadoopConfig") != null) {
            Map<String, Object> hadoopConfig = conf.getMap("hadoopConfig");
            for (Map.Entry<String, Object> entry : hadoopConfig.entrySet()) {
                if (entry.getValue() != null) {
                    // json lets a setting be a boolean or a number, hadoop takes only strings
                    hadoopConf.set(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }

            Object authentication = hadoopConfig.get("hadoop.security.authentication");
            if (authentication != null && "kerberos".equals(authentication.toString())) {
                String kerberosKeytabFilePath = StringUtils.trimToNull(conf.getString(KERBEROS_KEYTAB_FILE_PATH));
                if (kerberosKeytabFilePath == null) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, "kerberosKeytabFilePath is not set");
                }

                String kerberosPrincipal = StringUtils.trimToNull(conf.getString(KERBEROS_PRINCIPAL));
                if (kerberosPrincipal == null) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, "kerberosPrincipal is not set");
                }

                kerberosAuthentication(hadoopConf, kerberosPrincipal, kerberosKeytabFilePath);
            }
        }

        return switch (catalogType) {
            case "hadoop" -> new HadoopCatalog(hadoopConf, warehouse);
            case "hive" -> {
                String uri = StringUtils.trimToNull(conf.getString("uri"));
                if (uri == null) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, "uri is not set");
                }
                HiveCatalog hiveCatalog = new HiveCatalog();
                hiveCatalog.setConf(hadoopConf);
                Map<String, String> properties = new HashMap<>();
                properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
                properties.put(CatalogProperties.URI, uri);
                hiveCatalog.initialize("hive", properties);
                yield hiveCatalog;
            }
            default -> throw AddaxException.asAddaxException(CONFIG_ERROR, "not support catalogType:" + catalogType);
        };
    }
}
