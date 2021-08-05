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

package com.wgzhao.addax.common.compress;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipCycleOutputStream
        extends OutputStream
{

    private final ZipOutputStream zipOutputStream;

    public ZipCycleOutputStream(OutputStream out, String fileName)
            throws IOException
    {
        this.zipOutputStream = new ZipOutputStream(out);
        ZipEntry currentZipEntry = new ZipEntry(fileName);
        this.zipOutputStream.putNextEntry(currentZipEntry);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        byte[] data = {(byte) b};
        this.zipOutputStream.write(data, 0, data.length);
    }

    @Override
    public void close()
            throws IOException
    {
        this.zipOutputStream.closeEntry();
        this.zipOutputStream.close();
    }
}
