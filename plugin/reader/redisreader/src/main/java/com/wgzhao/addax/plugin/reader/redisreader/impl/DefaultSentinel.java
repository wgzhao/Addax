/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.reader.redisreader.impl;

import static com.moilioncircle.redis.replicator.util.Concurrents.terminateQuietly;
import static com.moilioncircle.redis.replicator.util.Strings.isEquals;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static redis.clients.jedis.Protocol.Command.UNSUBSCRIBE;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.replicator.Configuration;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisException;


/**
 * @author Leon Chen
 * @since 3.1.1
 */
public class DefaultSentinel implements Sentinel {

    protected static final Logger logger = LoggerFactory.getLogger(DefaultSentinel.class);

    protected volatile Jedis jedis;
    protected final String masterName;
    protected final List<HostAndPort> hosts;
    protected final Configuration configuration;
    protected final String channel = "+switch-master";
    protected AtomicBoolean running = new AtomicBoolean(true);
    protected final List<SentinelListener> listeners = new CopyOnWriteArrayList<>();
    protected final ScheduledExecutorService schedule = newSingleThreadScheduledExecutor();

    public DefaultSentinel(List<HostAndPort> hosts, String masterName, Configuration configuration) {
        this.hosts = hosts;
        this.masterName = masterName;
        this.configuration = configuration;
    }

    @Override
    public void open() throws IOException {
        await(schedule.scheduleWithFixedDelay(this::pulse, 0, 10, SECONDS));
    }

    @Override
    public void close() throws IOException {
        unsubscribe(channel);
        terminateQuietly(schedule, 0, MILLISECONDS);
    }

    @Override
    public boolean addSentinelListener(SentinelListener listener) {
        return this.listeners.add(listener);
    }

    @Override
    public boolean removeSentinelListener(SentinelListener listener) {
        return this.listeners.remove(listener);
    }

    protected void doCloseListener() {
        if (listeners.isEmpty()) return;
        for (SentinelListener listener : listeners) {
            listener.onClose(this);
        }
    }

    protected void doSwitchListener(HostAndPort host) {
        if (listeners.isEmpty()) return;
        for (SentinelListener listener : listeners) {
            listener.onSwitch(this, host);
        }
    }

    protected void pulse() {
        for (HostAndPort sentinel : hosts) {
            if (!this.running.get()) continue;
            try (final Jedis jedis = new Jedis(sentinel)) {
                List<String> list = jedis.sentinelGetMasterAddrByName(masterName);
                if (list == null || list.size() != 2) {
                    throw new JedisException("host: " + list);
                }
                String host = list.get(0);
                int port = Integer.parseInt(list.get(1));
                doSwitchListener(new HostAndPort(host, port));
                this.jedis = jedis;
                logger.info("subscribe sentinel {}", sentinel);
                jedis.subscribe(new PubSub(), this.channel);
            } catch (Throwable cause) {
                logger.warn("suspend sentinel {}, cause: {}", sentinel, cause);
            }
        }
    }

    protected class PubSub extends JedisPubSub {

        @Override
        public void onUnsubscribe(String channel, int channels) {
            running.set(false);
            doCloseListener();
        }

        @Override
        public void onMessage(String channel, String response) {
            try {
                final String[] messages = response.split(" ");
                if (messages.length <= 3) {
                    logger.error("failed to handle, response: {}", response);
                    return;
                }
                String prev = masterName, next = messages[0];
                if (!isEquals(prev, next)) {
                    logger.error("failed to match master, prev: {}, next: {}", prev, next);
                    return;
                }

                final String host = messages[3];
                final int port = parseInt(messages[4]);
                doSwitchListener(new HostAndPort(host, port));
            } catch (Throwable cause) {
                logger.error("failed to subscribe: {}, cause: {}", response, cause);
            }
        }
    }

    protected void unsubscribe(String channel) {
        for (int retry = 0; retry < 5; retry++) {
            if(!running.get()) break;
            if(jedis == null || !jedis.isConnected()) continue;
            run(() -> jedis.sendCommand(UNSUBSCRIBE, channel));
            sleep(1, SECONDS);
        }
    }

    protected <T> T run(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception ignore) {
            return null;
        }
    }

    protected void sleep(long time, TimeUnit unit) {
        try {
            unit.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void await(Future<?> future) {
        try {
            future.get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (CancellationException e) {
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void await(Future<?> future, long timeout, TimeUnit unit) {
        try {
            future.get(timeout, unit);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (CancellationException e) {
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}