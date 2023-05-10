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

package com.wgzhao.addax.common.util;

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
            InetAddress inetAddress = InetAddress.getLocalHost();
            ip = inetAddress.getHostAddress();
            hostname = inetAddress.getHostName();
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
                process.destroy();
                process = Runtime.getRuntime().exec("hostname");
                if (process.waitFor() == 0) {
                    hostname = (new String(IOUtils.toByteArray(process.getInputStream()), StandardCharsets.UTF_8)).trim();
                }
                process.destroy();
            }
            catch (Exception e) {
                log.warn("Failed to get hostname: {}", e.getMessage());
            }
        }
        IP = ip;
        HOSTNAME = hostname;
        log.info("IP {} HOSTNAME {}", IP, HOSTNAME);
    }
}
