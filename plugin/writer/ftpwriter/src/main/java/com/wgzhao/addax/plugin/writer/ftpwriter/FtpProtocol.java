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

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.wgzhao.addax.plugin.writer.ftpwriter.util.SftpHelperImpl;
import com.wgzhao.addax.plugin.writer.ftpwriter.util.StandardFtpHelperImpl;

import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

/**
 * The transfer protocols this writer speaks. Each one knows the port to use when the job does not
 * configure one and the helper implementation that talks to it, so the job and the task side do
 * not have to repeat the same protocol branches.
 */
public enum FtpProtocol
{
    /** Plain ftp. */
    FTP(FtpConstant.DEFAULT_FTP_PORT),
    /** Sftp over ssh. */
    SFTP(FtpConstant.DEFAULT_SFTP_PORT);

    private final int defaultPort;

    FtpProtocol(int defaultPort)
    {
        this.defaultPort = defaultPort;
    }

    /**
     * Parse the configured protocol name, it is matched case-insensitively.
     *
     * @param value the configured value
     * @return the matching protocol
     * @throws AddaxException if the value names no supported protocol
     */
    public static FtpProtocol of(String value)
    {
        for (FtpProtocol protocol : values()) {
            if (protocol.name().equalsIgnoreCase(value)) {
                return protocol;
            }
        }
        throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                "Only support ftp and sftp protocols, the " + value + " is not supported.");
    }

    /**
     * Create the helper for this protocol and log it in to the server.
     *
     * @param connection the connection settings
     * @return the logged-in helper
     */
    public IFtpHelper connect(FtpConnection connection)
    {
        IFtpHelper helper = switch (this) {
            case FTP -> new StandardFtpHelperImpl();
            case SFTP -> new SftpHelperImpl();
        };
        helper.loginFtpServer(connection);
        return helper;
    }

    /**
     * The port to connect to when the job does not configure one.
     *
     * @return the default port of this protocol
     */
    public int getDefaultPort()
    {
        return defaultPort;
    }
}
