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

package com.wgzhao.addax.plugin.writer.s3writer.writer;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;

public class TextWriter
        implements IFormatWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(TextWriter.class);
    private char fieldDelimiter;
    private String nullFormat;
    private String dateFormat;
    private String encoding;
    private String bucket;
    private String object;
    private int maxFileSize;
    private List<String> header;
    private S3Client s3Client;

    public TextWriter()
    {
    }

    public char getFieldDelimiter()
    {
        return fieldDelimiter;
    }

    public TextWriter setFieldDelimiter(char fieldDelimiter)
    {
        this.fieldDelimiter = fieldDelimiter;
        return this;
    }

    public String getNullFormat()
    {
        return nullFormat;
    }

    public TextWriter setNullFormat(String nullFormat)
    {
        this.nullFormat = nullFormat;
        return this;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }

    public TextWriter setDateFormat(String dateFormat)
    {
        this.dateFormat = dateFormat;
        return this;
    }

    public String getEncoding()
    {
        return encoding;
    }

    public TextWriter setEncoding(String encoding)
    {
        this.encoding = encoding;
        return this;
    }

    public String getBucket()
    {
        return bucket;
    }

    public TextWriter setBucket(String bucket)
    {
        this.bucket = bucket;
        return this;
    }

    public String getObject()
    {
        return object;
    }

    public TextWriter setObject(String object)
    {
        this.object = object;
        return this;
    }

    public int getMaxFileSize()
    {
        return maxFileSize;
    }

    public TextWriter setMaxFileSize(int maxFileSize)
    {
        this.maxFileSize = maxFileSize;
        return this;
    }

    public List<String> getHeader()
    {
        return header;
    }

    public TextWriter setHeader(List<String> header)
    {
        this.header = header;
        return this;
    }

    public S3Client getS3Client()
    {
        return s3Client;
    }

    public TextWriter setS3Client(S3Client s3Client)
    {
        this.s3Client = s3Client;
        return this;
    }

    @Override
    public void init(Configuration config)
    {

    }

    @Override
    public void write(RecordReceiver lineReceiver, Configuration config, TaskPluginCollector taskPluginCollector)
    {

        // 设置每块字符串长度
        final int partSize = 1024 * 1024 * 10;
        long numberCalc = (this.maxFileSize * 1024 * 1024L) / partSize;
        final long maxPartNumber = numberCalc >= 1 ? numberCalc : 1;
        //warn: may be StringBuffer->StringBuilder
        Record record;

        LOG.info("Begin do write, each object's max file size is {}MB...", maxPartNumber * 10);
        // First create a multipart upload and get the upload id
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(object)
                .build();
        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = response.uploadId();
        int currPart = 1;
        List<CompletedPart> completedParts = new ArrayList<>();

        UploadPartRequest uploadPartRequest;
        uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucket)
                .key(object)
                .uploadId(uploadId)
                .partNumber(currPart).build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Charset charset = Charset.forName(encoding);
        boolean needInit = true;
        while ((record = lineReceiver.getFromReader()) != null) {
            try {
                if (needInit && !header.isEmpty()) {
                    // write header
                    outputStream.write(String.join(String.valueOf(fieldDelimiter), header).getBytes(charset));
                    outputStream.write("\n".getBytes(charset));
                    needInit = false;
                }
                outputStream.write(record2String(record).getBytes(charset));
                outputStream.write("\n".getBytes(charset));

                if (outputStream.size() > partSize) {
                    String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(outputStream.toByteArray())).eTag();
                    CompletedPart completedPart = CompletedPart.builder().partNumber(currPart).eTag(etag).build();
                    completedParts.add(completedPart);
                    currPart += 1;
                    outputStream.reset();

                    uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucket)
                            .key(object)
                            .uploadId(uploadId)
                            .partNumber(currPart).build();
                    needInit = true;
                }
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR, e.getMessage());
            }
        }
        // remain bytes
        if (outputStream.size() > 0) {
            String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(outputStream.toByteArray())).eTag();
            CompletedPart completedPart = CompletedPart.builder().partNumber(currPart).eTag(etag).build();
            completedParts.add(completedPart);
            outputStream.reset();
        }
        if (!completedParts.isEmpty()) {
            // Finally, call completeMultipartUpload operation to tell S3 to merge all uploaded
            // parts and finish the multipart operation.
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest =
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(object)
                            .uploadId(uploadId)
                            .multipartUpload(completedMultipartUpload)
                            .build();

            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
            LOG.info("end do write");
        }
        else {
            LOG.info("no content do write");
        }
    }

    private String record2String(Record record)
    {
        StringJoiner sj = new StringJoiner(this.fieldDelimiter + "");
        int columnNum = record.getColumnNumber();
        for (int i = 0; i < columnNum; i++) {
            Column column = record.getColumn(i);
            if (column == null || column.asString() == null) {
                sj.add(this.nullFormat);
            }
            assert column != null;
            Column.Type type = column.getType();
            if (type == Column.Type.DATE) {
                SimpleDateFormat sdf = new SimpleDateFormat(this.dateFormat);
                sj.add(sdf.format(column.asDate()));
            }
            else {
                sj.add(column.asString());
            }
        }
        return sj.toString();
    }
}
