/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class RetryUtil
{

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);

    private static final long MAX_SLEEP_MILLISECOND = 256 * 1000L;

    private RetryUtil() {}

    /**
     * 重试次数工具方法.
     *
     * @param callable 实际逻辑
     * @param retryTimes 最大重试次数
     * @param sleepTimeInMilliSecond 运行失败后休眠对应时间再重试
     * @param exponential 休眠时间是否指数递增
     * @param <T> 返回值类型
     * @return 经过重试的callable的执行结果
     * @throws Exception 运行异常
     */

    /**
     *  This method is used to execute the callable with retry.
     * @param callable the actual logic
     * @param retryTimes the maximum number of retries
     * @param sleepTimeInMilliSecond the time to sleep after the operation fails before retrying
     * @param exponential whether the sleep time is exponentially increased
     * @param <T> the return type
     * @throws Exception if the operation fails
     */
    public static <T> T executeWithRetry(Callable<T> callable,
            int retryTimes,
            long sleepTimeInMilliSecond,
            boolean exponential)
            throws Exception
    {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
    }

    /**
     *  This method is used to execute the callable with retry.
     * @param callable the actual logic
     * @param retryTimes the maximum number of retries
     * @param sleepTimeInMilliSecond the time to sleep after the operation fails before retrying
     * @param exponential whether the sleep time is exponentially increased
     * @param retryExceptionClass the exception types that trigger a retry
     * @param <T> the return type
     * @throws Exception if the operation fails
     */
    public static <T> T executeWithRetry(Callable<T> callable,
            int retryTimes,
            long sleepTimeInMilliSecond,
            boolean exponential,
            List<Class<?>> retryExceptionClass)
            throws Exception
    {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, retryExceptionClass);
    }

    /**
     *  It is used to execute and retry in an external thread. Each execution must be completed within timeoutMs, otherwise it is considered a failure.
     *  limit conditions: can only interrupt threads when blocking
     * @param callable the actual logic
     * @param retryTimes the maximum number of retries
     * @param sleepTimeInMilliSecond the time to sleep after the operation fails before retrying
     * @param exponential whether the sleep time is exponentially increased
     * @param timeoutMs the timeout for the callable to execute, in milliseconds
     * @param executor the thread pool for executing asynchronous operations
     * @return the result of the callable after retry
     * @param <T> the return type
     * @throws Exception if the operation fails
     */
    public static <T> T asyncExecuteWithRetry(Callable<T> callable,
            int retryTimes,
            long sleepTimeInMilliSecond,
            boolean exponential,
            long timeoutMs,
            ThreadPoolExecutor executor)
            throws Exception
    {
        Retry retry = new AsyncRetry(timeoutMs, executor);
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
    }

    /**
     *  Create an asynchronous thread pool. The characteristics are as follows:
     *  The core size is 0, and there are no threads initially, so there is no initial consumption.
     *  The maximum size is 5, with a maximum of five threads.
     *  The timeout is 60 seconds, and threads that are idle for more than 60 seconds will be recycled.
     *  Use SynchronousQueue, tasks are not queued, and there must be available threads to submit successfully,
     *  otherwise a RejectedExecutionException will be thrown.
     * @return the thread pool
     */
    public static ThreadPoolExecutor createThreadPoolExecutor()
    {
        return new ThreadPoolExecutor(0, 5,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
    }

    private static class Retry
    {

        public <T> T doRetry(Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond, boolean exponential, List<Class<?>> retryExceptionClass)
                throws Exception
        {

            if (null == callable) {
                throw new IllegalArgumentException("The parameter callable cannot be null.");
            }

            if (retryTimes < 1) {
                throw new IllegalArgumentException(String.format(
                        "The value [%d] of parameter retryTime cannot less than 1", retryTimes));
            }

            Exception saveException = null;
            for (int i = 0; i < retryTimes; i++) {
                try {
                    return call(callable);
                }
                catch (Exception e) {
                    saveException = e;
                    if (null != retryExceptionClass && !retryExceptionClass.isEmpty()) {
                        boolean needRetry = false;
                        for (Class<?> eachExceptionClass : retryExceptionClass) {
                            if (eachExceptionClass == e.getClass()) {
                                needRetry = true;
                                break;
                            }
                        }
                        if (!needRetry) {
                            throw saveException;
                        }
                    }

                    if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                        long startTime = System.currentTimeMillis();

                        long timeToSleep;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                        }
                        else {
                            timeToSleep = sleepTimeInMilliSecond;
                        }
                        if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                            timeToSleep = MAX_SLEEP_MILLISECOND;
                        }

                        try {
                            Thread.sleep(timeToSleep);
                        }
                        catch (InterruptedException ignored) {
                            // ignore interrupted exception
                        }

                        long realTimeSleep = System.currentTimeMillis() - startTime;

                        LOG.error("Exception when calling callable, Attempt retry {}. This retry waits {}ms" +
                                        ", actually waits {}ms, exception message: {}.",
                                i + 1, timeToSleep, realTimeSleep, e.getMessage());
                    }
                }
            }
            throw saveException;
        }

        protected <T> T call(Callable<T> callable)
                throws Exception
        {
            return callable.call();
        }
    }

    private static class AsyncRetry
            extends Retry
    {

        private final long timeoutMs;
        private final ThreadPoolExecutor executor;

        public AsyncRetry(long timeoutMs, ThreadPoolExecutor executor)
        {
            this.timeoutMs = timeoutMs;
            this.executor = executor;
        }

        /**
         *  Asynchronously execute the task using the thread pool provided, and wait.
         *  The future.get() method waits for the specified number of milliseconds. If the task ends within the timeout period, it returns normally.
         *  If an exception is thrown (possibly due to a timeout, an exception during execution, or being canceled or interrupted by another thread), log it and throw an exception.
         *  In both normal and abnormal cases, check whether the task has ended. If it has not ended, cancel the task. The cancel parameter is true,
         *  which means that even if the task is running, the thread will be interrupted.
         */
        @Override
        protected <T> T call(Callable<T> callable)
                throws Exception
        {
            Future<T> future = executor.submit(callable);
            try {
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            }
            finally {
                if (!future.isDone()) {
                    future.cancel(true);
                    LOG.warn("A try-once task was not completed. Cancel it. Active count: {}.", executor.getActiveCount());
                }
            }
        }
    }
}
