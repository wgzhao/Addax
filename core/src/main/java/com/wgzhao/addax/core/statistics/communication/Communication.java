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

import com.wgzhao.addax.common.base.BaseObject;
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
    Map<String, List<String>> message;
    private Map<String, Number> counter;
    // Running status
    private State state;
    private Throwable throwable;
    private long timestamp;

    public Communication()
    {
        this.init();
    }

    public Communication(Communication communication)
    {
        for (Map.Entry<String, Number> entry : this.counter.entrySet()) {
            String key = entry.getKey();
            Number value = entry.getValue();
            if (value instanceof Long) {
                communication.setLongCounter(key, (Long) value);
            }
            else if (value instanceof Double) {
                communication.setDoubleCounter(key, (Double) value);
            }
        }

        communication.setState(this.state, true);
        communication.setThrowable(this.throwable, true);
        communication.setTimestamp(this.timestamp);

        /*
         * clone message
         */
        if (this.message != null) {
            for (Map.Entry<String, List<String>> entry : this.message.entrySet()) {
                String key = entry.getKey();
                List<String> value = new ArrayList<>(entry.getValue());
                communication.getMessage().put(key, value);
            }
        }
    }

    public synchronized void reset()
    {
        this.init();
    }

    private void init()
    {
        this.counter = new ConcurrentHashMap<>();
        this.state = State.RUNNING;
        this.throwable = null;
        this.message = new ConcurrentHashMap<>();
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

        long value = this.getLongCounter(key);

        this.counter.put(key, value + deltaValue);
    }

    public synchronized Communication mergeFrom(Communication otherComm)
    {
        if (otherComm == null) {
            return this;
        }

        /*
         * counter的合并，将otherComm的值累加到this中，不存在的则创建
         * 同为long
         */
        for (Entry<String, Number> entry : otherComm.getCounter().entrySet()) {
            String key = entry.getKey();
            Number otherValue = entry.getValue();
            if (otherValue == null) {
                continue;
            }

            Number value = this.counter.get(key);
            if (value == null) {
                value = otherValue;
            }
            else {
                if (value instanceof Long && otherValue instanceof Long) {
                    value = value.longValue() + otherValue.longValue();
                }
                else {
                    value = value.doubleValue() + value.doubleValue();
                }
            }

            this.counter.put(key, value);
        }

        // 合并state
        mergeStateFrom(otherComm);

        /*
         * 合并throwable，当this的throwable为空时，
         * 才将otherComm的throwable合并进来
         */
        this.throwable = this.throwable == null ? otherComm.getThrowable() : this.throwable;

        /*
         * timestamp是整个一次合并的时间戳，单独两两communication不作合并
         */

        /*
         * message的合并采取求并的方式，即全部累计在一起
         */
        for (Entry<String, List<String>> entry : otherComm.getMessage().entrySet()) {
            String key = entry.getKey();
            List<String> valueList = this.message.computeIfAbsent(key, k -> new ArrayList<>());

            valueList.addAll(entry.getValue());
        }

        return this;
    }

    /**
     * 合并state，优先级： ( Failed | Killed )  &gt; Running &gt; Success
     * 这里不会出现 Killing 状态，killing 状态只在 Job 自身状态上才有.
     *
     * @param otherComm communication
     * @return the communication state
     */
    public synchronized State mergeStateFrom(Communication otherComm)
    {
        State retState = this.getState();
        if (otherComm == null) {
            return retState;
        }

        if (this.state == State.FAILED || otherComm.getState() == State.FAILED
                || this.state == State.KILLED || otherComm.getState() == State.KILLED) {
            retState = State.FAILED;
        }
        else if (this.state.isRunning() || otherComm.state.isRunning()) {
            retState = State.RUNNING;
        }

        this.setState(retState);
        return retState;
    }

    public synchronized boolean isFinished()
    {
        return this.state == State.SUCCEEDED || this.state == State.FAILED
                || this.state == State.KILLED;
    }
}
