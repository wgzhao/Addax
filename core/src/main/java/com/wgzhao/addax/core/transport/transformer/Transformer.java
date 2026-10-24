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

package com.wgzhao.addax.core.transport.transformer;

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

public abstract class Transformer
{
    private String transformerName;

    // the parameters are fixed for the lifetime of the instance: parse them on the
    // first use so the per-record path only reads fields and never the raw config
    private volatile boolean initialized;

    public String getTransformerName()
    {
        return transformerName;
    }

    public void setTransformerName(String transformerName)
    {
        this.transformerName = transformerName;
    }

    /**
     * Initialize the transformer with its configured parameters. The framework calls
     * this once when the transformer is built, so configuration errors surface before
     * any record is processed. Subclasses override {@link #doInit(Object[])}.
     *
     * <p>The default implementation is a no-op so that existing user-defined
     * transformers keep working without changes.
     *
     * @param paras transformer parameters
     */
    public final void init(Object[] paras)
    {
        if (initialized) {
            return;
        }
        synchronized (this) {
            if (initialized) {
                return;
            }
            doInit(paras);
            initialized = true;
        }
    }

    /**
     * Parse and validate the configured parameters. Called once per instance.
     *
     * @param paras transformer parameters
     */
    protected void doInit(Object[] paras)
    {
        // user-defined transformers keep the historical behavior of parsing in evaluate()
    }

    /**
     * @param record the record to be transformed
     * @param paras transformer parameters
     * @return record
     */
    public abstract Record evaluate(Record record, Object... paras);

    protected final int intParam(Object value, String paramName)
    {
        String text = value == null ? null : String.valueOf(value).trim();
        if (text == null || text.isEmpty()) {
            throw configError("the parameter [" + paramName + "] is required and must be an integer");
        }
        try {
            return Integer.parseInt(text);
        }
        catch (NumberFormatException e) {
            throw configError("the parameter [" + paramName + "] must be an integer, but was [" + value + "]");
        }
    }

    protected final String stringParam(Object value, String paramName)
    {
        if (value == null) {
            throw configError("the parameter [" + paramName + "] is required");
        }
        return String.valueOf(value);
    }

    protected final AddaxException configError(String message)
    {
        return AddaxException.asAddaxException(CONFIG_ERROR,
                getTransformerName() + ": " + message);
    }
}
