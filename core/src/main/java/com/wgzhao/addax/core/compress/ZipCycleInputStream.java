/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.compress;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipCycleInputStream
        extends InputStream
{

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
        // Locate the beginning of an entry stream
        while (true) {
            if (this.currentZipEntry == null) {
                this.currentZipEntry = this.zipInputStream.getNextEntry();
                if (this.currentZipEntry == null) {
                    return -1;
                }
            }

            // Skip directories; nested zips are not supported
            if (this.currentZipEntry.isDirectory()) {
                this.currentZipEntry = null;
                continue;
            }

            // Read one entry stream
            int result = this.zipInputStream.read();

            // If current entry stream ends, try next entry
            if (result == -1) {
                this.currentZipEntry = null;
                continue;
            }
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
