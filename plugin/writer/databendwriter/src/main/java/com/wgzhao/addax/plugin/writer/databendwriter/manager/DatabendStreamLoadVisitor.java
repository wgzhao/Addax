package com.wgzhao.addax.plugin.writer.databendwriter.manager;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.plugin.writer.databendwriter.DatabendWriterOptions;
import com.wgzhao.addax.plugin.writer.databendwriter.row.DatabendDelimiterParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class DatabendStreamLoadVisitor
{

    private static final Logger LOG = LoggerFactory.getLogger(DatabendStreamLoadVisitor.class);
    private final DatabendWriterOptions writerOptions;
    private long pos;

    public DatabendStreamLoadVisitor(DatabendWriterOptions writerOptions)
    {
        this.writerOptions = writerOptions;
    }

    public void doStreamLoad(DatabendFlushTuple flushData)
            throws IOException
    {
        long start = System.currentTimeMillis();
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the host in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
                .append("/v1/streaming_load")
                .toString();
        LOG.debug(String.format("Start to join batch data: rows[%d] bytes[%d].", flushData.getRows().size(), flushData.getBytes()));
        Map<String, Object> loadResult = doHttpPut(loadUrl, joinRows(flushData.getRows(), flushData.getBytes().intValue()));
        long cost = System.currentTimeMillis() - start;
        LOG.info("StreamLoad response: " + JSON.toJSONString(loadResult) + ", cost(ms): " + cost);
        final String keyStatus = "state";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            LOG.error("Data is [" + flushData.getRows().toString() + "]");
            throw new IOException("Unable to flush data to databend: unknown result status.");
        }
        if (!loadResult.get(keyStatus).equals("SUCCESS")) {
            throw new IOException("Failed to flush data to databend.\n" + JSON.toJSONString(loadResult));
        }
        else {
            List<String> hostList = writerOptions.getLoadUrlList();
            System.out.printf("\npos is %s\n", pos);
            if (pos > hostList.size()) {
                pos = 0;
            }
            else {
                pos += 1;
            }
        }
    }

    private String getAvailableHost()
    {
        List<String> hostList = writerOptions.getLoadUrlList();
        long tmp = pos + hostList.size();
        System.out.printf("pos is %s, tmp is [%s]", pos, tmp);
        for (; pos < tmp; pos++) {
            int a = (int) (pos % hostList.size());
            System.out.printf("{pos %% hostList.size()} is [%s], pos is %s", a, pos);
            System.out.printf("\nhostList.get(a) is [%s]\n", hostList.get(a));
            String host = new StringBuilder("http://").append(hostList.get((int) (pos % hostList.size()))).toString();
            if (tryHttpConnection(host)) {
                LOG.debug(String.format("Host [%s] can be connected, will use it", host));
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host)
    {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(1000);
            co.connect();
            co.disconnect();
            return true;
        }
        catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes)
    {
        String format = writerOptions.getStreamLoadFormat();
        if ("csv".equalsIgnoreCase(format)) {
            byte[] lineDelimiter = DatabendDelimiterParser.parse(writerOptions.getLinedelimiter(), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        throw new RuntimeException(String.format("Failed to join rows data, only support format 'csv', but get [%s]", format));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String loadUrl, byte[] data)
            throws IOException
    {
        LOG.info(String.format("Executing stream load to: '%s', size: '%s'", loadUrl, data.length));

        final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy()
        {
            @Override
            protected boolean isRedirectable(final String method)
            {
                return true;
            }

            @Override
            public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context)
                    throws ProtocolException
            {
                URI uri = this.getLocationURI(request, response, context);
                String method = request.getRequestLine().getMethod();
                if (method.equalsIgnoreCase("HEAD")) {
                    return new HttpHead(uri);
                }
                else if (method.equalsIgnoreCase("GET")) {
                    return new HttpGet(uri);
                }
                else {
                    int status = response.getStatusLine().getStatusCode();
                    return status == 307 ? RequestBuilder.copy(request).setUri(uri).build() : new HttpGet(uri);
                }
            }
        });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            List<String> cols = writerOptions.getColumns();

            String field_delimiter = writerOptions.getFielddelimiter();
            String line_delimiter = writerOptions.getLinedelimiter();
            String database = writerOptions.getDatabase();
            String table = writerOptions.getTable();
            if (null != cols && !cols.isEmpty()) {
                if (cols.size() == 1 && "*".equals(cols.get(0))) {
                    String insert = String.format("insert into %s.%s file_format = (type = 'CSV' field_delimiter = '%s' record_delimiter = '%s') ", database, table, field_delimiter, line_delimiter);
                    httpPut.setHeader("insert_sql", insert);
                }
                else {
                    String columns = "(" + String.join(",", cols) + ")";
                    String insert = String.format("insert into %s.%s %s file_format = (type = 'CSV' field_delimiter = '%s' record_delimiter = '%s') ", database, table, columns, field_delimiter, line_delimiter);
                    httpPut.setHeader("insert_sql", insert);
                }
            }
            else {
                throw new IOException("COLUMN is null or empty");
            }

            httpPut.setHeader("Authorization", getBasicAuthHeader(writerOptions.getUsername(), writerOptions.getPassword()));

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            builder.addBinaryBody("AddaxUpFile", data, ContentType.DEFAULT_BINARY, "addax_up_file");
            HttpEntity entity = builder.build();
            httpPut.setEntity(entity);

            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    return null;
                }
                return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    private String getBasicAuthHeader(String username, String password)
    {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth, Charset.defaultCharset());
    }

    private HttpEntity getHttpEntity(CloseableHttpResponse resp)
    {
        int code = resp.getStatusLine().getStatusCode();
        if (200 != code) {
            LOG.warn("Request failed with code:{}", code);
            return null;
        }
        HttpEntity respEntity = resp.getEntity();
        if (null == respEntity) {
            LOG.warn("Request failed with empty response.");
            return null;
        }
        return respEntity;
    }
}
