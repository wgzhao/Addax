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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

public class Communication
        extends BaseObject
{

    // Message about the task is given to the job
    // Made final and initialized at declaration to ensure the map reference never changes.
    // CopyOnWriteArrayList values let plugin threads append while a collector thread merges.
    private final Map<String, List<String>> message = new ConcurrentHashMap<>();
    // Per-key lock-free accumulators. A key lives in exactly one of the two maps
    // (callers must not mix setLongCounter and setDoubleCounter on the same key).
    private final Map<String, AtomicLong> longCounters = new ConcurrentHashMap<>();
    private final Map<String, DoubleAdder> doubleCounters = new ConcurrentHashMap<>();
    // Running status
    // volatile: the timestamp/throwable written by a failing task thread must be
    // visible to the task-group loop that observes state == FAILED
    private volatile State state;
    private volatile Throwable throwable;
    private volatile long timestamp;

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
        for (Map.Entry<String, AtomicLong> entry : source.longCounters.entrySet()) {
            this.setLongCounter(entry.getKey(), entry.getValue().get());
        }
        for (Map.Entry<String, DoubleAdder> entry : source.doubleCounters.entrySet()) {
            this.setDoubleCounter(entry.getKey(), entry.getValue().doubleValue());
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
        this.longCounters.clear();
        this.doubleCounters.clear();
        this.state = State.RUNNING;
        this.throwable = null;
        this.message.clear();
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Snapshot of all counters as a plain map. Callers only iterate the result;
     * for live reads use the typed accessors.
     */
    public Map<String, Number> getCounter()
    {
        Map<String, Number> snapshot = new HashMap<>(longCounters.size() + doubleCounters.size());
        snapshot.putAll(longCounters);
        snapshot.putAll(doubleCounters);
        return snapshot;
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

    public void addMessage(String key, String value)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of the added message cannot be empty.");
        List<String> valueList = this.message.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());

        valueList.add(value);
    }

    public Long getLongCounter(String key)
    {
        AtomicLong value = this.longCounters.get(key);
        return value == null ? 0 : value.get();
    }

    public void setLongCounter(String key, long value)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of setting counter can not be empty.");
        this.longCounters.put(key, new AtomicLong(value));
    }

    /**
     * Register a live accumulator so reads reflect its value without per-update writes.
     * The caller keeps updating the {@link AtomicLong}; this communication only references it.
     */
    public void putLongCounter(String key, AtomicLong source)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of setting counter can not be empty.");
        Validate.isTrue(source != null, "The counter source can not be null.");
        this.longCounters.put(key, source);
    }

    public Double getDoubleCounter(String key)
    {
        DoubleAdder value = this.doubleCounters.get(key);
        return value == null ? 0.0d : value.doubleValue();
    }

    public void setDoubleCounter(String key, double value)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of setting counter can not be empty.");
        DoubleAdder adder = new DoubleAdder();
        adder.add(value);
        this.doubleCounters.put(key, adder);
    }

    public void increaseCounter(String key, long deltaValue)
    {
        Validate.isTrue(StringUtils.isNotBlank(key), "The key of the added counter can not be empty.");

        // lock-free per-key accumulation: no monitor, no boxing on the hot path
        this.longCounters.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(deltaValue);
    }

    public void mergeFrom(Communication otherComm)
    {
        if (otherComm == null) {
            return;
        }

        // merge counters: add otherComm's value to this, create if not exist
        for (Map.Entry<String, AtomicLong> entry : otherComm.longCounters.entrySet()) {
            this.longCounters.computeIfAbsent(entry.getKey(), k -> new AtomicLong())
                    .addAndGet(entry.getValue().get());
        }
        for (Map.Entry<String, DoubleAdder> entry : otherComm.doubleCounters.entrySet()) {
            this.doubleCounters.computeIfAbsent(entry.getKey(), k -> new DoubleAdder())
                    .add(entry.getValue().doubleValue());
        }

        mergeStateFrom(otherComm);

        this.throwable = this.throwable == null ? otherComm.getThrowable() : this.throwable;

        // combine all messages
        for (Map.Entry<String, List<String>> entry : otherComm.getMessage().entrySet()) {
            String key = entry.getKey();
            List<String> valueList = this.message.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());

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
