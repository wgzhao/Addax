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

package com.wgzhao.addax.plugin.writer.rediswriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.wgzhao.addax.common.base.Key.CONNECTION;

public class RedisWriter
        extends Writer
{

    public static class Task
            extends Writer.Task
    {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final AtomicBoolean FLUSH_FLAG = new AtomicBoolean(false);

        private final Map<Integer, Jedis> cluster = new HashMap<>();
        private final Map<Jedis, AtomicLong> nodeCounterMap = new HashMap<>();

        private Jedis jedis;

        private long batchSize = 1000L;

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            Configuration pluginJobConf = this.getPluginJobConf();

            boolean isCluster = pluginJobConf.getBool("redisCluster", false);
            if (isCluster) {
                this.clusterWrite(lineReceiver);
            }
            else {
                this.standaloneWrite(lineReceiver);
            }
        }

        @Override
        public void init()
        {
            Configuration pluginJobConf = this.getPluginJobConf();
            Configuration connection = pluginJobConf.getConfiguration(CONNECTION);
            boolean isCluster = pluginJobConf.getBool("redisCluster", false);
            int timeout = pluginJobConf.getInt("timeout", 60000);
            this.batchSize = pluginJobConf.getLong("batchSize", 1000L);

            URI uri = URI.create(connection.getString("uri"));
            String host = uri.getHost();
            int port = uri.getPort();
            this.jedis = new Jedis(host, port, timeout, timeout);

            if (isCluster) {
                StringBuilder sb = new StringBuilder("\r\nRedis Cluster node assign\r\n");
                List<Object> slots = this.jedis.clusterSlots();

                for (Object slot : slots) {
                    List<Object> list = (List<Object>) slot;
                    //slot begin node
                    Long start = (Long) list.get(0);
                    //slot end node
                    Long end = (Long) list.get(1);
                    //slot node host
                    List hostInfo = (List) list.get(2);

                    String nodeHost = new String((byte[]) hostInfo.get(0));
                    Long nodePort = (Long) hostInfo.get(1);
                    Jedis node = new Jedis(nodeHost, nodePort.intValue(), timeout, timeout);
                    for (int i = start.intValue(); i <= end.intValue(); i++) {
                        this.cluster.put(i, node);
                    }

                    this.nodeCounterMap.put(node, new AtomicLong());
                    sb.append(nodeHost)
                            .append(":")
                            .append(nodePort)
                            .append("\t")
                            .append("slot:")
                            .append(start)
                            .append("-").append(end)
                            .append("\r\n");
                }
                LOG.info(sb.toString());
            }
            else {
                String auth = connection.getString("auth");
                if (StringUtils.isNotBlank(auth)) {
                    this.jedis.auth(auth);
                }
            }

            prepare();
        }

        @Override
        public void prepare()
        {
            Boolean isFlushDB = getPluginJobConf().getBool("flushDB", false);
            if (isFlushDB) {
                flushDB();
            }
        }

        public void destroy()
        {
            if (this.jedis != null) {
                this.jedis.close();
            }

            this.nodeCounterMap.clear();
            for (Jedis cJedis : new HashSet<>(this.cluster.values())) {
                cJedis.close();
            }

            this.cluster.clear();
        }

        private void standaloneWrite(RecordReceiver lineReceiver)
        {
            AtomicLong counter = new AtomicLong(0L);
            Client client = this.jedis.getClient();
            Record fromReader;
            while ((fromReader = lineReceiver.getFromReader()) != null) {
                int db = fromReader.getColumn(0).asLong().intValue();
                long expire = fromReader.getColumn(2).asLong();
                byte[] key = fromReader.getColumn(3).toString().getBytes();
                byte[] value = string2byte(fromReader.getColumn(4).toString());
                client.select(db);
                restore(client, key, value, expire, counter);
            }

            if (counter.get() % this.batchSize != 0L) {
                flushAndCheckReply(client);
            }
        }

        private void clusterWrite(RecordReceiver lineReceiver)
        {

            Record fromReader;
            while ((fromReader = lineReceiver.getFromReader()) != null) {
                Column expireColumn = fromReader.getColumn(2);
                Column keyColumn = fromReader.getColumn(3);
                Column valueColumn = fromReader.getColumn(4);
                Long expire = expireColumn.asLong();
                byte[] key = keyColumn.asBytes();
                byte[] value = valueColumn.asBytes();
                int slot = JedisClusterCRC16.getSlot(key);
                Jedis node = this.cluster.get(slot);
                Client client = node.getClient();
                AtomicLong nodeCounter = nodeCounterMap.get(node);
                restore(client, key, value, expire, nodeCounter);
            }

            for (Entry<Jedis, AtomicLong> entry : nodeCounterMap.entrySet()) {
                AtomicLong nodeCounter = entry.getValue();
                Jedis node = entry.getKey();
                if (nodeCounter.get() % this.batchSize != 0L) {
                    Client client = node.getClient();
                    flushAndCheckReply(client);
                }
            }
        }

        private byte[] string2byte(String str)
        {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         try {
             byteArrayOutputStream.write(str.getBytes());
             return byteArrayOutputStream.toByteArray();
         }
         catch (IOException e) {
             return str.getBytes();
         }
        }

        private void restore(Client client, byte[] key, byte[] value, long expire, AtomicLong currentCounter)
        {

            client.restore(key, 0L, value);

            if (expire > 0) {
                client.expireAt(key, expire);
            }

            long count = currentCounter.incrementAndGet();

            if (count % this.batchSize == 0L) {
                flushAndCheckReply(client);
            }
        }

        private void flushDB()
        {
            synchronized (FLUSH_FLAG) {
                if (FLUSH_FLAG.get()) {
                    return;
                }

                boolean isCluster = getPluginJobConf().getBool("redisCluster", false);
                Client client = null;
                if (isCluster) {
                    for (Jedis cJedis : new HashSet<>(cluster.values())) {
                        client = cJedis.getClient();
                        jedis.flushAll();
                    }
                }
                else {
                    if (this.jedis != null) {
                        client = jedis.getClient();
                        jedis.flushAll();
                    }
                }
                if (client != null ) {
                    LOG.info("redis client: {}: {}", client.getHost(), client.getPort());
                }
                FLUSH_FLAG.set(true);
            }
        }

        private void flushAndCheckReply(Client client)
        {
            List<Object> allReply = client.getObjectMultiBulkReply();
            for (Object o : allReply) {
                if (o instanceof JedisDataException) {
                    throw (JedisDataException) o;
                }
            }
        }
    }

    public static class Job
            extends Writer.Job
    {

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return Collections.singletonList(getPluginJobConf());
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