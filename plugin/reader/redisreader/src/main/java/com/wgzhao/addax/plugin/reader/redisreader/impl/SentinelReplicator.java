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

import com.moilioncircle.redis.replicator.CloseListener;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.ExceptionListener;
import com.moilioncircle.redis.replicator.RedisSocketReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Replicators;
import com.moilioncircle.redis.replicator.Status;
import com.moilioncircle.redis.replicator.StatusListener;
import com.moilioncircle.redis.replicator.cmd.Command;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.CommandParser;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.io.RawByteListener;
import com.moilioncircle.redis.replicator.rdb.RdbVisitor;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.wgzhao.addax.plugin.reader.redisreader.util.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static com.moilioncircle.redis.replicator.util.Concurrents.terminateQuietly;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SentinelReplicator
        implements Replicator, SentinelListener
{

    protected static final Logger logger = LoggerFactory.getLogger(SentinelReplicator.class);

    private HostAndPort prev;
    private final Sentinel sentinel;
    private final RedisSocketReplicator replicator;
    protected final ExecutorService executors = newSingleThreadExecutor();

    public SentinelReplicator(List<HostAndPort> hosts, String name, Configuration configuration)
    {
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(configuration);
        this.replicator = new RedisSocketReplicator("", 1, configuration);
        this.sentinel = new DefaultSentinel(hosts, name, configuration);
        this.sentinel.addSentinelListener(this);
    }

    @Override
    public void open()
            throws IOException
    {
        this.sentinel.open();
    }

    @Override
    public void close()
            throws IOException
    {
        this.sentinel.close();
    }

    @Override
    public boolean addEventListener(EventListener listener)
    {
        return replicator.addEventListener(listener);
    }

    @Override
    public boolean removeEventListener(EventListener listener)
    {
        return replicator.removeEventListener(listener);
    }

    @Override
    public boolean addRawByteListener(RawByteListener listener)
    {
        return replicator.addRawByteListener(listener);
    }

    @Override
    public boolean removeRawByteListener(RawByteListener listener)
    {
        return replicator.removeRawByteListener(listener);
    }

    @Override
    public boolean addCloseListener(CloseListener listener)
    {
        return replicator.addCloseListener(listener);
    }

    @Override
    public boolean removeCloseListener(CloseListener listener)
    {
        return replicator.removeCloseListener(listener);
    }

    @Override
    public boolean addExceptionListener(ExceptionListener listener)
    {
        return replicator.addExceptionListener(listener);
    }

    @Override
    public boolean removeExceptionListener(ExceptionListener listener)
    {
        return replicator.removeExceptionListener(listener);
    }

    @Override
    public boolean addStatusListener(StatusListener listener)
    {
        return replicator.addStatusListener(listener);
    }

    @Override
    public boolean removeStatusListener(StatusListener listener)
    {
        return replicator.removeStatusListener(listener);
    }

    @Override
    public void builtInCommandParserRegister()
    {
        replicator.builtInCommandParserRegister();
    }

    @Override
    public CommandParser<? extends Command> getCommandParser(CommandName command)
    {
        return replicator.getCommandParser(command);
    }

    @Override
    public <T extends Command> void addCommandParser(CommandName command, CommandParser<T> parser)
    {
        replicator.addCommandParser(command, parser);
    }

    @Override
    public CommandParser<? extends Command> removeCommandParser(CommandName command)
    {
        return replicator.removeCommandParser(command);
    }

    @Override
    public ModuleParser<? extends Module> getModuleParser(String moduleName, int moduleVersion)
    {
        return replicator.getModuleParser(moduleName, moduleVersion);
    }

    @Override
    public <T extends Module> void addModuleParser(String moduleName, int moduleVersion, ModuleParser<T> parser)
    {
        replicator.addModuleParser(moduleName, moduleVersion, parser);
    }

    @Override
    public ModuleParser<? extends Module> removeModuleParser(String moduleName, int moduleVersion)
    {
        return replicator.removeModuleParser(moduleName, moduleVersion);
    }

    @Override
    public void setRdbVisitor(RdbVisitor rdbVisitor)
    {
        replicator.setRdbVisitor(rdbVisitor);
    }

    @Override
    public RdbVisitor getRdbVisitor()
    {
        return replicator.getRdbVisitor();
    }

    @Override
    public boolean verbose()
    {
        return replicator.verbose();
    }

    @Override
    public Status getStatus()
    {
        return replicator.getStatus();
    }

    @Override
    public Configuration getConfiguration()
    {
        return replicator.getConfiguration();
    }

    @Override
    public void onSwitch(Sentinel sentinel, HostAndPort next)
    {
        if (prev == null || !prev.equals(next)) {
            logger.info("Sentinel switch master to [{}]", next);
            Replicators.closeQuietly(replicator);
            executors.submit(() -> {
                Reflections.setField(replicator, "host", next.getHost());
                Reflections.setField(replicator, "port", next.getPort());
                Replicators.openQuietly(replicator);
            });
        }
        prev = next;
    }

    @Override
    public void onClose(Sentinel sentinel)
    {
        Replicators.closeQuietly(replicator);
        terminateQuietly(executors, getConfiguration().getConnectionTimeout(), MILLISECONDS);
    }
}