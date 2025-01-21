package com.wgzhao.addax.plugin.writer.paimonwriter;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wgzhao.addax.common.spi.ErrorCode.LOGIN_ERROR;

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
        if(options.get("warehouse").startsWith("hdfs://")){

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
