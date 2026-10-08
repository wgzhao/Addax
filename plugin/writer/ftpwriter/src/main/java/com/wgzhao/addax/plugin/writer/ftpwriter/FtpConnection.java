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

package com.wgzhao.addax.plugin.writer.ftpwriter;

import com.wgzhao.addax.core.util.Configuration;

import static com.wgzhao.addax.core.base.Key.PASSWORD;
import static com.wgzhao.addax.core.base.Key.USERNAME;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.plugin.writer.ftpwriter.FtpKey.CONNECT_PATTERN;
import static com.wgzhao.addax.plugin.writer.ftpwriter.FtpKey.HOST;
import static com.wgzhao.addax.plugin.writer.ftpwriter.FtpKey.KEY_PASS;
import static com.wgzhao.addax.plugin.writer.ftpwriter.FtpKey.KEY_PATH;
import static com.wgzhao.addax.plugin.writer.ftpwriter.FtpKey.PORT;
import static com.wgzhao.addax.plugin.writer.ftpwriter.FtpKey.TIMEOUT;

/**
 * The connection settings of one (s)ftp server. The job and the task side both read them from
 * their configuration with {@link #from(Configuration, FtpProtocol)}, so the two sides cannot
 * drift apart.
 *
 * @param host address of the server
 * @param port port of the server, the protocol's default when not configured
 * @param username user name
 * @param password password, may be null when a private key is used
 * @param keyPath path of the private key, only used by sftp
 * @param keyPass passphrase of the private key, may be null
 * @param timeout connect and socket timeout in milliseconds
 * @param connectPattern PORT or PASV, only used by ftp
 */
public record FtpConnection(String host, int port, String username, String password, String keyPath,
        String keyPass, int timeout, String connectPattern)
{
    /**
     * Read the connection settings from a job or slice configuration.
     *
     * @param configuration the configuration to read
     * @param protocol the protocol the settings belong to
     * @return the connection settings
     */
    public static FtpConnection from(Configuration configuration, FtpProtocol protocol)
    {
        return new FtpConnection(
                configuration.getNecessaryValue(HOST, REQUIRED_VALUE),
                configuration.getInt(PORT, protocol.getDefaultPort()),
                configuration.getNecessaryValue(USERNAME, REQUIRED_VALUE),
                configuration.getString(PASSWORD),
                configuration.getString(KEY_PATH, null),
                configuration.getString(KEY_PASS, null),
                configuration.getInt(TIMEOUT, FtpConstant.DEFAULT_TIMEOUT_MS),
                protocol == FtpProtocol.FTP
                        ? configuration.getString(CONNECT_PATTERN, FtpConstant.DEFAULT_FTP_CONNECT_PATTERN)
                        : null);
    }
}
