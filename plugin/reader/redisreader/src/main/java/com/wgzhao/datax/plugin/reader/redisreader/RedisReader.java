package com.wgzhao.datax.plugin.reader.redisreader;

import com.wgzhao.datax.common.element.BytesColumn;
import com.wgzhao.datax.common.element.LongColumn;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbVisitor;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_HASH_ZIPMAP;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_QUICKLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_LIST_ZIPLIST;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_MODULE_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_SET_INTSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STREAM_LISTPACKS;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_STRING;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_2;
import static com.moilioncircle.redis.replicator.Constants.RDB_TYPE_ZSET_ZIPLIST;

public class RedisReader
        extends Reader
{

    public static class Task
            extends Reader.Task
    {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        /**
         * 包含key正则
         */
        private final List<Pattern> includePatterns = new ArrayList<>();

        /**
         * 排除key正则
         */
        private final List<Pattern> excludePatterns = new ArrayList<>();

        /**
         * 包含DB
         */
        private final Set<Integer> includeDB = new HashSet<>();

        /**
         * 记录redis 比较大的key 用于展示
         */
        private final Map<String, Integer> bigKey = new TreeMap<>();
        /**
         * 用于记录数据类型分布
         */
        private final Map<String, Long> collectTypeMap = new HashMap<>();
        /**
         * value达到64m阀值，将记录该key
         */
        private int keyThresholdLength = 64 * 1024 * 1024;

        @Override
        public void startRead(RecordSender recordSender)
        {
            Configuration pluginJobConf = getPluginJobConf();
            List<Object> connections = pluginJobConf.getList("connection");
            try {
                for (Object obj : connections) {
                    Map connection = (Map) obj;
                    URI uri = URI.create(connection.get("uri").toString());
                    File file = new File(UUID.randomUUID() + ".rdb");
                    if ("http".equals(uri.getScheme())) {
                        this.download(uri, file);
                    }
                    else if ("tcp".equals(uri.getScheme())) {
                        String auth = "";
                        if (connection.get("auth") != null) {
                            auth = "?authPassword=" + connection.get("auth");
                        }

                        this.dump(uri.toString().replace("tcp://", "redis://") + auth, file);
                    }
                    else {
                        file = new File(uri);
                    }

                    LOG.info("loading {} ", file.getAbsolutePath());
                    RedisReplicator r = new RedisReplicator(file, FileType.RDB, com.moilioncircle.redis.replicator.Configuration.defaultSetting());
                    r.addEventListener((replicator, event) -> {
                        if (event instanceof KeyStringValueString) {
                            KeyStringValueString dkv = (KeyStringValueString) event;
                            long dbNumber = dkv.getDb().getDbNumber();
                            int rdbType = dkv.getValueRdbType();
                            byte[] key = dkv.getKey();
                            byte[] value = dkv.getValue();
                            long expire = dkv.getExpiredMs() == null ? 0 : dkv.getExpiredMs();

                            //记录较大的key
                            recordBigKey(dbNumber, rdbType, key, value);

                            //记录数据类型
                            collectType(rdbType);

                            if (Task.this.matchDB((int) dbNumber) && Task.this.matchKey(key)) {
                                Record record = recordSender.createRecord();
                                record.addColumn(new LongColumn(dbNumber));
                                record.addColumn(new LongColumn(rdbType));
                                record.addColumn(new LongColumn(expire));
                                record.addColumn(new BytesColumn(key));
                                record.addColumn(new BytesColumn(value));
                                recordSender.sendToWriter(record);
                            }
                        }
                        else {
                            LOG.warn("non-keyvalue type not support yet");
                        }
                    });
                    r.open();
                    r.close();
                    // delete temporary local file
                    Files.deleteIfExists(Paths.get(file.getAbsolutePath()));
                } // end for
            }
            catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public void init()
        {
            Configuration pluginJobConf = this.getPluginJobConf();
            List<Object> include = pluginJobConf.getList("include");
            List<Object> exclude = pluginJobConf.getList("exclude");
            List<Object> db = pluginJobConf.getList("db");
            this.keyThresholdLength = pluginJobConf.getInt("keyThresholdLength", 64 * 1024 * 1024);
            if (include != null) {
                for (Object reg : include) {
                    Pattern pattern = Pattern.compile(reg.toString());
                    includePatterns.add(pattern);
                }
            }

            if (exclude != null) {
                for (Object reg : exclude) {
                    Pattern pattern = Pattern.compile(reg.toString());
                    excludePatterns.add(pattern);
                }
            }

            if (db != null) {
                for (Object num : db) {
                    includeDB.add(Integer.parseInt(String.valueOf(num)));
                }
            }
        }

        private void recordBigKey(Long db, int type, byte[] key, byte[] value)
        {
            if (value.length > keyThresholdLength) {
                bigKey.put(db + "\t" + new String(key, StandardCharsets.UTF_8), value.length);
            }
        }

        @Override
        public void destroy()
        {
            StringBuilder sb = new StringBuilder("Redis中较大的key:\n");

            for (Map.Entry<String, Integer> entry : bigKey.entrySet()) {
                sb.append(entry.getKey())
                        .append("\t")
                        .append(entry.getValue())
                        .append("\n");
            }

            LOG.info(sb.toString());

            sb = new StringBuilder("Redis数据类型分布:\n");

            for (Map.Entry<String, Long> entry : collectTypeMap.entrySet()) {
                sb.append(entry.getKey())
                        .append("\t")
                        .append(entry.getValue())
                        .append("\n");
            }

            LOG.info(sb.toString());
        }

        private boolean matchKey(byte[] bytes)
        {
            if (includePatterns.isEmpty() && excludePatterns.isEmpty()) {
                return true;
            }

            String key = new String(bytes, StandardCharsets.UTF_8);

            for (Pattern pattern : includePatterns) {
                boolean isMatch = pattern.matcher(key).find();
                if (isMatch) {
                    return true;
                }
            }

            for (Pattern pattern : excludePatterns) {
                boolean isMatch = pattern.matcher(key).find();
                if (isMatch) {
                    return false;
                }
            }

            return false;
        }

        /**
         * 判断是否包含相关db
         *
         * @param db db index
         * @return true if match else false
         */
        private boolean matchDB(int db)
        {
            return this.includeDB.isEmpty() || this.includeDB.contains(db);
        }

        /**
         * 通过sync命令远程下载redis server rdb文件
         *
         * @param uri uri
         * @param outFile file which dump to
         * @throws IOException file not found
         * @throws URISyntaxException uri parser error
         */
        private void dump(String uri, File outFile)
                throws IOException, URISyntaxException
        {
            OutputStream out = new BufferedOutputStream(new FileOutputStream(outFile));
            RawByteListener rawByteListener = rawBytes -> {
                try {
                    out.write(rawBytes);
                }
                catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            };
            Replicator replicator = new RedisReplicator(uri);
            replicator.setRdbVisitor(new SkipRdbVisitor(replicator));
            replicator.addEventListener((replicator1, event) -> {
                if (event instanceof PreRdbSyncEvent) {
                    replicator1.addRawByteListener(rawByteListener);
                }

                if (event instanceof PostRdbSyncEvent) {
                    replicator1.removeRawByteListener(rawByteListener);

                    try {
                        out.close();
                        replicator1.close();
                    }
                    catch (IOException e) {
                        LOG.warn(e.getMessage(), e);
                    }
                }
            });
            replicator.open();
        }

        /**
         * 下载远程rdb文件
         *
         * @param uri uri
         * @param outFile file will be written
         * @throws IOException when can not reach to uri
         */
        private void download(URI uri, File outFile)
                throws IOException
        {
            CloseableHttpClient httpClient = this.getHttpClient();
            CloseableHttpResponse response = httpClient.execute(new HttpGet(uri));
            HttpEntity entity = response.getEntity();
            InputStream in = entity.getContent();
            byte[] bytes = new byte[4096 * 1000];

            int len;
            try (FileOutputStream out = new FileOutputStream(outFile)) {
                while ((len = in.read(bytes)) != -1) {
                    out.write(bytes, 0, len);
                    out.flush();
                }
                in.close();
            }
        }

        private CloseableHttpClient getHttpClient()
        {
            return HttpClientBuilder.create().build();
        }

        private void collectType(int type)
        {
            String name = getTypeName(type);
            Long count = collectTypeMap.get(name);
            if (count == null) {
                collectTypeMap.put(name, 1L);
            }
            else {
                collectTypeMap.put(name, count + 1);
            }
        }

        private String getTypeName(int type)
        {
            switch (type) {
                case RDB_TYPE_STRING:
                    return "string";
                case RDB_TYPE_LIST:
                    return "list";
                case RDB_TYPE_SET:
                    return "set";
                case RDB_TYPE_ZSET:
                    return "zset";
                case RDB_TYPE_ZSET_2:
                    return "zset2";
                case RDB_TYPE_HASH:
                    return "hash";
                case RDB_TYPE_HASH_ZIPMAP:
                    return "hash_zipmap";
                case RDB_TYPE_LIST_ZIPLIST:
                    return "list_ziplist";
                case RDB_TYPE_SET_INTSET:
                    return "set_intset";
                case RDB_TYPE_ZSET_ZIPLIST:
                    return "zset_ziplist";
                case RDB_TYPE_HASH_ZIPLIST:
                    return "hash_ziplist";
                case RDB_TYPE_LIST_QUICKLIST:
                    return "list_quicklist";
                case RDB_TYPE_MODULE:
                    return "module";
                case RDB_TYPE_MODULE_2:
                    return "module2";
                case RDB_TYPE_STREAM_LISTPACKS:
                    return "stream_listpacks";
                default:
                    return "other";
            }
        }
    }

    public static class Job
            extends Reader.Job
    {

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return Collections.singletonList(super.getPluginJobConf());
        }

        @Override
        public void init()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
