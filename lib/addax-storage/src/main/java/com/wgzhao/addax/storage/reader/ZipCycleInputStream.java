/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.storage.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipCycleInputStream
        extends InputStream
{
    private static final Logger LOG = LoggerFactory
            .getLogger(ZipCycleInputStream.class);

    private final ZipInputStream zipInputStream;
    private ZipEntry currentZipEntry;

    public ZipCycleInputStream(InputStream in)
    {
        this.zipInputStream = new ZipInputStream(in);
    }

    @Override
    public int read()
            throws IOException
    {
        // 定位一个Entry数据流的开头
        if (null == this.currentZipEntry) {
            this.currentZipEntry = this.zipInputStream.getNextEntry();
            if (null == this.currentZipEntry) {
                return -1;
            }
            else {
                LOG.info("Validate zipEntry with name: {}",
                        this.currentZipEntry.getName());
            }
        }

        // 不支持zip下的嵌套, 对于目录跳过
        if (this.currentZipEntry.isDirectory()) {
            LOG.warn("meet a directory {}, ignore...",
                    this.currentZipEntry.getName());
            this.currentZipEntry = null;
            return this.read();
        }

        // 读取一个Entry数据流
        int result = this.zipInputStream.read();

        // 当前Entry数据流结束了, 需要尝试下一个Entry
        if (-1 == result) {
            this.currentZipEntry = null;
            return this.read();
        }
        else {
            return result;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        this.zipInputStream.close();
    }
}
