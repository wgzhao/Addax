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

package com.wgzhao.addax.core.statistics.communication;

import com.wgzhao.addax.core.base.BaseObject;
import com.wgzhao.addax.core.meta.State;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class Communication
        extends BaseObject
{

    // Message about the task is given to the job
    // Made final and initialized at declaration to ensure the map reference never changes.
    private final Map<String, List<String>> message = new ConcurrentHashMap<>();
    private final Map<String, Number> counter = new ConcurrentHashMap<>();
    // Running status
    private State state;
    private Throwable throwable;
    private long timestamp;

    /**
     * Create a new Communication with default values.
     */
    public Communication()
    {
        this.init();
    }

    /**
     * Deep-copy constructor. Clone all counters, state, throwable and messages
     * from the given source communication into this instance.
     *
     * @param source the source communication to copy from
     */
    public Communication(Communication source)
    {
        this.init();
        if (source == null) {
            return;
        }
        // copy counters
        for (Map.Entry<String, Number> entry : source.getCounter().entrySet()) {
            String key = entry.getKey();
            Number value = entry.getValue();
            if (value instanceof Long) {
                this.setLongCounter(key, value.longValue());
            }
            else if (value instanceof Double) {
                this.setDoubleCounter(key, value.doubleValue());
            }
        }
        // copy state/throwable/timestamp
        this.setState(source.getState(), true);
        this.setThrowable(source.getThrowable(), true);
        this.setTimestamp(source.getTimestamp());

        // clone messages
        for (Map.Entry<String, List<String>> entry : source.message.entrySet()) {
            String key = entry.getKey();
            List<String> value = new ArrayList<>(entry.getValue());
            this.getMessage().put(key, value);
        }
    }

    public synchronized void reset()
    {
        this.init();
    }

    private void init()
    {
        // clear the maps instead of reassigning to keep the references final
        this.counter.clear();
        this.state = State.RUNNING;
        this.throwable = null;
        this.message.clear();
        this.timestamp = System.currentTimeMillis();
    }

    public Map<String, Number> getCounter()
    {
        return this.counter;
    }

    public synchronized State getState()
    {
        return this.state;
    }

    public synchronized void setState(State state)
    {
        setState(state, false);
    }

    public synchronized void setState(State state, boolean isForce)
    {
        if (!isForce && this.state == State.FAILED) {
            return;
        }

        this.state = state;
    }

    public Throwable getThrowable()
    {
        return this.throwable;
    }

    public void setThrowable(Throwable throwable)
    {
        setThrowable(throwable, false);
    }

    public synchronized String getThrowableMessage()
    {
        return this.throwable == null ? "" : this.throwable.getMessage();
    }

    public synchronized void setThrowable(Throwable throwable, boolean isForce)
    {
        if (isForce) {
            this.throwable = throwable;
        }
        else {
            this.throwable = this.throwable == null ? throwable : this.throwable;
        }
    }

    public long getTimestamp()
    {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public Map<String, List<String>> getMessage()
    {
        return this.message;
    }

    public List<String> getMessage(String key)
    {
        return message.get(key);
    }

    public synchronized void addMessage(String key, String value)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of the added message cannot be empty.");
        List<String> valueList = this.message.computeIfAbsent(key, k -> new ArrayList<>());

        valueList.add(value);
    }

    public synchronized Long getLongCounter(String key)
    {
        Number value = this.counter.get(key);
        return value == null ? 0 : value.longValue();
    }

    public synchronized void setLongCounter(String key, long value)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of setting counter can not be empty.");
        this.counter.put(key, value);
    }

    public synchronized Double getDoubleCounter(String key)
    {
        Number value = this.counter.get(key);

        return value == null ? 0.0d : value.doubleValue();
    }

    public synchronized void setDoubleCounter(String key, double value)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of setting counter can not be empty.");
        this.counter.put(key, value);
    }

    public synchronized void increaseCounter(String key, long deltaValue)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of the added counter can not be empty.");

        // Use Map.merge to atomically update numeric counters. Primitive deltaValue is autoboxed to Long.
        this.counter.merge(key, deltaValue, (oldVal, newVal) -> Long.sum(oldVal.longValue(), newVal.longValue()));
    }

    public synchronized void mergeFrom(Communication otherComm)
    {
        if (otherComm == null) {
            return;
        }

        // merge counter, add otherComm's value to this, create if not exist
        for (Entry<String, Number> entry : otherComm.getCounter().entrySet()) {
            String key = entry.getKey();
            Number otherValue = entry.getValue();
            if (otherValue == null) {
                continue;
            }

            // Use merge to combine numbers while preserving integer/double semantics
            this.counter.merge(key, otherValue, (current, incoming) -> {
                // both integer-like -> keep as Long
                if (current instanceof Long && incoming instanceof Long) {
                    return Long.sum(current.longValue(), incoming.longValue());
                }
                else {
                    // otherwise, use double arithmetic
                    return Double.sum(current.doubleValue(), incoming.doubleValue());
                }
            });
        }

        mergeStateFrom(otherComm);

        this.throwable = this.throwable == null ? otherComm.getThrowable() : this.throwable;

        // combine all messages
        for (Entry<String, List<String>> entry : otherComm.getMessage().entrySet()) {
            String key = entry.getKey();
            List<String> valueList = this.message.computeIfAbsent(key, k -> new ArrayList<>());

            valueList.addAll(entry.getValue());
        }
    }

    /**
     * Merge state, priority: (Failed | Killed) &gt; Running &gt; Success
     * Killing state only exists in Job's own state.
     *
     * @param otherComm communication
     */
    public synchronized void mergeStateFrom(Communication otherComm)
    {
        State retState = this.getState();
        if (otherComm == null) {
            return;
        }

        if (this.state == State.FAILED || otherComm.getState() == State.FAILED
                || this.state == State.KILLED || otherComm.getState() == State.KILLED) {
            retState = State.FAILED;
        }
        else if (this.state.isRunning() || otherComm.state.isRunning()) {
            retState = State.RUNNING;
        }

        this.setState(retState);
    }

    public synchronized boolean isFinished()
    {
        return this.state == State.SUCCEEDED || this.state == State.FAILED
                || this.state == State.KILLED;
    }
}
