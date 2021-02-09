package com.wgzhao.datax.common.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

/**
 * Created by liqiang on 15/8/25.
 */
public class HostUtils
{

    private HostUtils() {}

    public static final String IP;
    public static final String HOSTNAME;
    private static final Logger log = LoggerFactory.getLogger(HostUtils.class);
    private static final String UNKNOWN = "UNKNOWN";

    static {
        String ip = UNKNOWN;
        String hostname = UNKNOWN;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();
            hostname = addr.getHostName();
        }
        catch (UnknownHostException e) {
            log.error("Can't find out address: {}", e.getMessage());
        }
        if (ip.equals("127.0.0.1") || ip.equals("::1") || ip.equals(UNKNOWN)) {
            try {
                Process process = Runtime.getRuntime().exec("hostname -i");
                if (process.waitFor() == 0) {
                    ip = new String(IOUtils.toByteArray(process.getInputStream()), StandardCharsets.UTF_8);
                }
                process = Runtime.getRuntime().exec("hostname");
                if (process.waitFor() == 0) {
                    hostname = (new String(IOUtils.toByteArray(process.getInputStream()), StandardCharsets.UTF_8)).trim();
                }
            }
            catch (Exception e) {
                log.warn("get hostname failed {}", e.getMessage());
            }
        }
        IP = ip;
        HOSTNAME = hostname;
        log.info("IP {} HOSTNAME {}", IP, HOSTNAME);
    }
}
