package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration originConfig = null;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> result = new ArrayList<>();
            result.add(this.originConfig);
            return result;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private Configuration readerSliceConfig = null;
        private String url;
        private URI fullUri;
        private URIBuilder uriBuilder;
        private HttpHost proxy;
        private String username;
        private String password;
        private String token;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.username = readerSliceConfig.getString(Key.USERNAME, null);
            this.password = readerSliceConfig.getString(Key.PASSWORD, null);
            this.token = readerSliceConfig.getString(Key.TOKEN, null);
            Configuration conn =
                    readerSliceConfig.getListConfiguration(Key.CONNECTION).get(0);
            uriBuilder = new URIBuilder(URI.create(conn.getString(Key.URL)));

            if (readerSliceConfig.getString(Key.PROXY, null) != null) {
                // set proxy
                Configuration proxyConf = conn.getConfiguration(Key.PROXY);
                String address = proxyConf.getString(Key.ADDRESS);
                if (address.startsWith("socks")) {
                    throw DataXException.asDataXException(
                            HttpReaderErrorCode.NOT_SUPPORT, "sockes 代理暂时不支持"
                    );
                }
                proxy = new HttpHost(address);
            }
            Map<String, Object> requestParams =
                    readerSliceConfig.getMap(Key.REQUEST_PARAMETERS, new HashMap<>());
            requestParams.forEach((k, v) -> uriBuilder.setParameter(k, v.toString()));
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            Map<String, Object> headers = readerSliceConfig.getMap(Key.HEADERS, new HashMap<>());
            String method = readerSliceConfig.getString(Key.METHOD, "get");
            CloseableHttpResponse response = null;
            try {
                if ("get".equalsIgnoreCase(method)) {
                    HttpGet request = new HttpGet(uriBuilder.build());
                    headers.forEach((k, v) -> request.setHeader(k, v.toString()));
                    CloseableHttpClient httpClient = HttpClients.createDefault();
                    response = httpClient.execute(request);
                }
                else if ("post".equalsIgnoreCase(method)) {
                    HttpPost request = new HttpPost(uriBuilder.build());
                    headers.forEach((k, v) -> request.setHeader(k, v.toString()));
                    CloseableHttpClient httpClient = HttpClients.createDefault();
                    response = httpClient.execute(request);
                }
                else {
                    throw DataXException.asDataXException(
                            HttpReaderErrorCode.ILLEGAL_VALUE, "不支持的请求模式: " + method
                    );
                }
                HttpEntity entity = response.getEntity();
                String encoding = readerSliceConfig.getString(Key.ENCODING, null);
                Charset charset;
                if (encoding != null) {
                    charset = Charset.forName(encoding);
                }
                else {
                    charset = StandardCharsets.UTF_8;
                }

                String json = EntityUtils.toString(entity, charset);
                JSONArray jsonArray = null;
                String key = readerSliceConfig.get(Key.RESULT_KEY, null);
                Object object;
                if (key != null) {
                    object = JSON.parseObject(json).get(key);
                }
                else {
                    object = JSON.parse(json);
                }
                // 需要判断返回的结果仅仅是一条记录还是多条记录，如果是一条记录，则是一个map
                // 否则是一个array
                if (object instanceof JSONArray) {
                    jsonArray = JSON.parseArray(object.toString());
                }
                else if (object instanceof JSONObject) {
                    jsonArray = new JSONArray();
                    jsonArray.add(object);
                }
                if (jsonArray == null || jsonArray.isEmpty()) {
                    // empty result
                    return;
                }
                List<String> columns = readerSliceConfig.getList(Key.COLUMN, String.class);
                if (columns == null || columns.isEmpty()) {
                    throw DataXException.asDataXException(
                            HttpReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.COLUMN + "] is not set."
                    );
                }
                Record record;
                if (columns.size() == 1 && "*".equals(columns.get(0))) {
                    log.info("get all keys beacause you setup columns with \"*\"");
                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject jsonObject = jsonArray.getJSONObject(i);
                        record = recordSender.createRecord();
                        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                            if (entry.getValue() == null) {
                                record.addColumn(new StringColumn(null));
                            }
                            else {
                                record.addColumn(new StringColumn(entry.getValue().toString()));
                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                }
                else {
                    // get specified key instead of all
                    // first, check key exists or not ?
                    JSONObject jsonObject = jsonArray.getJSONObject(0);
                    for (String k : columns) {
                        if (!jsonObject.containsKey(k)) {
                            throw DataXException.asDataXException(
                                    HttpReaderErrorCode.ILLEGAL_VALUE,
                                    "您尝试从结果中获取key为 '" + k + "'的结果，但实际结果中不存在该key值"
                            );
                        }
                    }
                    for (int i = 0; i < jsonArray.size(); i++) {
                        jsonObject = jsonArray.getJSONObject(i);
                        record = recordSender.createRecord();
                        for (String k : columns) {
                            Object v = jsonObject.get(k);
                            if (v == null) {
                                record.addColumn(new StringColumn(null));
                            }
                            else {
                                record.addColumn(new StringColumn(v.toString()));
                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                }
            }
            catch (URISyntaxException | IOException e) {
                throw DataXException.asDataXException(
                        HttpReaderErrorCode.ILLEGAL_VALUE, e.getMessage()
                );
            }
        }
    }
}
