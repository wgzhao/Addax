package com.wgzhao.addax.plugin.writer.icebergwriter;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.wgzhao.addax.common.base.Key.KERBEROS_KEYTAB_FILE_PATH;
import static com.wgzhao.addax.common.base.Key.KERBEROS_PRINCIPAL;
import static com.wgzhao.addax.common.spi.ErrorCode.LOGIN_ERROR;

public class IcebergHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergHelper.class);

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

    public static Catalog getCatalog(Configuration conf) throws Exception {

        String catalogType = conf.getString("catalogType");
        if (catalogType == null || catalogType.trim().isEmpty()) {
            throw new RuntimeException("catalogType is not set");
        }
        catalogType = catalogType.trim();

        String warehouse = conf.getString("warehouse");
        if (warehouse == null || warehouse.trim().isEmpty()) {
            throw new RuntimeException("warehouse is not set");
        }

        org.apache.hadoop.conf.Configuration hadoopConf = null;

        if (conf.getConfiguration("hadoopConfig") != null) {
            Map<String,Object> hadoopConfig = conf.getMap("hadoopConfig");

            hadoopConf = new org.apache.hadoop.conf.Configuration();

            for (String key : hadoopConfig.keySet()) {
                hadoopConf.set(key, (String)hadoopConfig.get(key));
            }


            String authentication = (String)hadoopConfig.get("hadoop.security.authentication");

            if ("kerberos".equals(authentication)) {
                String kerberosKeytabFilePath = conf.getString(KERBEROS_KEYTAB_FILE_PATH);
                if(kerberosKeytabFilePath ==null || kerberosKeytabFilePath.trim().isEmpty()){
                    throw new RuntimeException("kerberosKeytabFilePath is not set");
                } else {
                    kerberosKeytabFilePath = kerberosKeytabFilePath.trim();
                }

                String kerberosPrincipal = conf.getString(KERBEROS_PRINCIPAL);
                if(kerberosPrincipal ==null || kerberosPrincipal.trim().isEmpty()){
                    throw new RuntimeException("kerberosPrincipal is not set");
                } else {
                    kerberosPrincipal = kerberosPrincipal.trim();
                }
                IcebergHelper.kerberosAuthentication(hadoopConf, kerberosPrincipal, kerberosKeytabFilePath);
            }
        }
        switch (catalogType) {
            case "hadoop":
                return new HadoopCatalog(hadoopConf, warehouse);
            case "hive":
                String uri = conf.getString("uri");
                if (uri == null || uri.trim().isEmpty()) {
                    throw new RuntimeException("uri is not set");
                }
                HiveCatalog hiveCatalog = new HiveCatalog();
                hiveCatalog.setConf(hadoopConf);
                Map<String, String> properties = new HashMap<String, String>();
                properties.put("warehouse", warehouse);
                properties.put("uri", uri);

                hiveCatalog.initialize("hive", properties);
                return hiveCatalog;
        }

        throw new RuntimeException("not support catalogType:" + catalogType);
    }

}
