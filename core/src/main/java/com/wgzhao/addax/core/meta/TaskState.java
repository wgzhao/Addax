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

package com.wgzhao.addax.core.meta;

/**
 * The lifecycle of a task -- or of an aggregate of tasks -- as an immutable value:
 * the status, the reason it left {@link State#RUNNING}, and when that status took
 * effect.
 *
 * <p>Every party that used to assign the status field of a communication (the runner
 * of either side of a task, the task-group loop, the hung-task monitor, the
 * communicators that aggregate) now sends an event -- {@link #succeeded()},
 * {@link #failed(Throwable)} -- or merges a value ({@link #merge(TaskState)}). The
 * rules that used to be spread over those callers and their comments live here:
 *
 * <ul>
 *   <li><b>FAILED is absorbing.</b> The reader and the writer of a task share one
 *   state and run concurrently, so a writer finishing after the reader blew up would
 *   otherwise report the task as successful and lose the failure.</li>
 *   <li><b>The first reason wins.</b> The first failure recorded is the cause that
 *   tore the task down; whatever its teardown throws afterwards is a consequence.
 *   A failure without a reason stays representable: an attempt can be cancelled
 *   before anything was thrown.</li>
 *   <li><b>{@link #EMPTY} is the identity of {@link #merge}.</b> Merging no task at
 *   all leaves nothing unfinished, so the aggregate of nothing is SUCCEEDED, and
 *   merging it with any state yields that state. Container and job state are folded
 *   from this value, which is why no caller has to remember a seed.</li>
 * </ul>
 *
 * <p>Status, reason and timestamp are published together in a single reference write,
 * so a reader that observes FAILED also observes the reason for it. Writing the
 * status and the cause as two fields (the previous API) made that an ordering rule
 * that every writer had to know.
 */
public final class TaskState
{
    /**
     * The state of an aggregate that covers no task at all. SUCCEEDED is the only
     * value that can serve as the identity of the merge: FAILED and RUNNING both
     * absorb it. The timestamp is meaningless here -- an aggregate's reporting
     * timestamp is its own, not the state's.
     */
    public static final TaskState EMPTY = new TaskState(State.SUCCEEDED, null, 0L);

    private final State status;

    private final Throwable failure;

    private final long changedAt;

    private TaskState(State status, Throwable failure, long changedAt)
    {
        this.status = status;
        this.failure = failure;
        this.changedAt = changedAt;
    }

    /**
     * The state of a task that has been registered but has not reported anything yet.
     */
    public static TaskState running()
    {
        return new TaskState(State.RUNNING, null, System.currentTimeMillis());
    }

    /**
     * Mark the task as finished successfully. A task that already failed stays failed.
     */
    public TaskState succeeded()
    {
        if (this.status == State.FAILED) {
            return this;
        }

        return new TaskState(State.SUCCEEDED, this.failure, System.currentTimeMillis());
    }

    /**
     * Mark the task as failed, keeping the reason of the first failure.
     *
     * @param cause why it failed; may be null when the caller has nothing to report
     */
    public TaskState failed(Throwable cause)
    {
        return new TaskState(State.FAILED, this.failure == null ? cause : this.failure,
                System.currentTimeMillis());
    }

    /**
     * Remember why, without deciding anything about the status: an already recorded
     * reason is kept, and the status is left alone. Only the reporting path needs
     * this -- a snapshot of a job whose failing task has already been retried away
     * still has to show the error, but a later snapshot must not resurrect a failure
     * that has since been retried.
     *
     * @param cause the reason to record if none was recorded yet
     */
    public TaskState recordFailure(Throwable cause)
    {
        if (this.failure != null || cause == null) {
            return this;
        }

        return new TaskState(this.status, cause, this.changedAt);
    }

    /**
     * Combine this state with another one: the more severe status wins, and the reason
     * of the first failure of either side is kept.
     *
     * @param other the state to fold in, may be null
     */
    public TaskState merge(TaskState other)
    {
        if (other == null) {
            return this;
        }

        State severity = priority(other.status) > priority(this.status) ? other.status : this.status;
        Throwable reason = this.failure == null ? other.failure : this.failure;
        long when = Math.max(this.changedAt, other.changedAt);

        return new TaskState(severity, reason, when);
    }

    /**
     * FAILED outranks RUNNING outranks SUCCEEDED. Neither the declaration order of
     * {@link State} nor its {@code value()} ranks them, so the order is stated here.
     */
    private static int priority(State status)
    {
        return switch (status) {
            case FAILED -> 2;
            case RUNNING -> 1;
            case SUCCEEDED -> 0;
        };
    }

    /** The current status. */
    public State getStatus()
    {
        return this.status;
    }

    /** The reason of the first failure, or null if none was recorded. */
    public Throwable getFailure()
    {
        return this.failure;
    }

    /** When this status took effect; for a failure, the moment it was recorded. */
    public long getChangedAt()
    {
        return this.changedAt;
    }

    /** Whether the task is done: nothing will report for it any more. */
    public boolean isFinished()
    {
        return this.status == State.SUCCEEDED || this.status == State.FAILED;
    }

    @Override
    public String toString()
    {
        // the exception type is part of the answer: "FAILED(AddaxException: ...)" says
        // more than the message alone, which can be null
        return this.failure == null ? this.status.toString() : this.status + "(" + this.failure + ")";
    }
}
