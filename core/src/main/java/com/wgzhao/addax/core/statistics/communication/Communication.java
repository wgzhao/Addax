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
import com.wgzhao.addax.core.meta.TaskState;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
    // The lifecycle of the task, or of the aggregate this communication stands for,
    // as one immutable value: a writer never mutates what a reader is looking at, and
    // the status can never be observed without the reason that produced it.
    // An atomic reference rather than a volatile field because a transition has to be
    // a compare-and-set: FAILED is absorbing, and a plain read-modify-write would let
    // a concurrent success overwrite a failure published in between.
    private final AtomicReference<TaskState> taskState = new AtomicReference<>(TaskState.running());
    // When this communication was last sampled, used for the speed and rate deltas of
    // a reported aggregate. This is not part of the state machine: the reporting
    // timestamp of a single task is never read.
    private volatile long timestamp;

    /**
     * Create a new Communication with default values: RUNNING, no counters, no messages.
     */
    public Communication()
    {
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * A communication that has merged nothing yet: empty counters, no messages, and
     * the identity state of {@link TaskState#EMPTY}. Merging the communications of a
     * container into it yields that container's state, so the caller never has to
     * remember which state to seed the aggregate with.
     */
    public static Communication newAggregate()
    {
        Communication communication = new Communication();
        communication.taskState.set(TaskState.EMPTY);

        return communication;
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

    /** The status of the task, or of the aggregate, that this communication stands for. */
    public State getState()
    {
        return this.taskState.get().getStatus();
    }

    /** The lifecycle value behind {@link #getState()}: status, reason and when. */
    public TaskState getTaskState()
    {
        return this.taskState.get();
    }

    /** The reason of the first failure, or null if none was recorded. */
    public Throwable getThrowable()
    {
        return this.taskState.get().getFailure();
    }

    /**
     * Event: this side of the task finished its work.
     *
     * <p>The stage counter is a progress metric, not a verdict, so it counts a write
     * stage that completed even when the other side had already failed the task.
     */
    public void markSuccess()
    {
        this.increaseCounter(CommunicationTool.STAGE, 1);
        this.taskState.updateAndGet(TaskState::succeeded);
    }

    /**
     * Event: this side of the task failed.
     *
     * @param cause why it failed
     */
    public void markFailure(Throwable cause)
    {
        this.taskState.updateAndGet(state -> state.failed(cause));
    }

    /**
     * Record why, without deciding anything about the status. Only the reporting path
     * needs this; the rules are in {@link TaskState#recordFailure(Throwable)}.
     *
     * @param cause the reason to record if none was recorded yet
     */
    public void recordFailure(Throwable cause)
    {
        this.taskState.updateAndGet(state -> state.recordFailure(cause));
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

        // the merge rules live in TaskState; the counters above and the messages below
        // have no ordering constraint against it, they are independent accumulators
        this.taskState.updateAndGet(state -> state.merge(otherComm.getTaskState()));

        // combine all messages
        for (Map.Entry<String, List<String>> entry : otherComm.getMessage().entrySet()) {
            String key = entry.getKey();
            List<String> valueList = this.message.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());

            valueList.addAll(entry.getValue());
        }
    }

    /** Whether this task, or everything this aggregate covers, has finished. */
    public boolean isFinished()
    {
        return this.taskState.get().isFinished();
    }
}
