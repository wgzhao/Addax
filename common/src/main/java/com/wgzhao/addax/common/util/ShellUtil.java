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

package com.wgzhao.addax.common.util;

import com.wgzhao.addax.common.exception.AddaxException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class ShellUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(ShellUtil.class);

    public static void exec(String command, boolean ignoreError)
    {
        CommandLine cmdLine = CommandLine.parse(command);
        DefaultExecutor executor = DefaultExecutor.builder().get();
        LOG.info("Running command: {}", command);
        try {
            int ret = executor.execute(cmdLine);
            if (ret != 0) {
                if (ignoreError) {
                    LOG.warn("Command failed with return code: {}", ret);
                }
                else {
                    throw AddaxException.asAddaxException(EXECUTE_FAIL, "Command failed with return code: " + ret);
                }
            }
        }
        catch (Exception e) {
            if (ignoreError) {
                LOG.warn("Error running command: {}", command, e);
            }
            else {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
            }
        }
    }

    public static void exec(String command)
    {
        exec(command, false);
    }
}
