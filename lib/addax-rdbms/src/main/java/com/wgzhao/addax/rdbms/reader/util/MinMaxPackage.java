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

package com.wgzhao.addax.rdbms.reader.util;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Package class for storing minimum and maximum values of a primary key column
 * along with its data type information. Used for splitting table reads across multiple tasks.
 */
public class MinMaxPackage
{
    /**
     * Enumeration of supported primary key data types for splitting.
     */
    public enum PkType
    {
        /** Long integer types */
        LONG,
        /** String/character types */
        STRING,
        /** Monte Carlo sampling method */
        MONTE_CARLO,
        /** Floating-point numeric types */
        FLOAT
    }

    private Object min;
    private Object max;
    private PkType type;

    /**
     * Default constructor initializing all fields to null.
     */
    public MinMaxPackage()
    {
        this.min = null;
        this.max = null;
        this.type = null;
    }

    /**
     * Gets the minimum value of the primary key range.
     *
     * @return The minimum value
     */
    public Object getMin()
    {
        return min;
    }

    /**
     * Sets the minimum value of the primary key range.
     *
     * @param min The minimum value to set
     */
    public void setMin(Object min)
    {
        this.min = min;
    }

    /**
     * Gets the maximum value of the primary key range.
     *
     * @return The maximum value
     */
    public Object getMax()
    {
        return max;
    }

    /**
     * Sets the maximum value of the primary key range.
     *
     * @param max The maximum value to set
     */
    public void setMax(Object max)
    {
        this.max = max;
    }

    /**
     * Gets the primary key data type.
     *
     * @return The data type of the primary key
     */
    public PkType getType()
    {
        return type;
    }

    /**
     * Sets the primary key data type.
     *
     * @param type The data type to set
     */
    public void setType(PkType type)
    {
        this.type = type;
    }

    /**
     * Checks if the primary key type is a long integer.
     *
     * @return true if the type is LONG, false otherwise
     */
    public boolean isLong()
    {
        return type == PkType.LONG;
    }

    /**
     * Checks if the primary key type is a floating-point number.
     *
     * @return true if the type is FLOAT, false otherwise
     */
    public boolean isFloat()
    {
        return type == PkType.FLOAT;
    }

    /**
     * Checks if the primary key type is numeric (either long or float).
     *
     * @return true if the type is numeric, false otherwise
     */
    public boolean isNumeric()
    {
        return isLong() || isFloat();
    }

    /**
     * Checks if the primary key type is a string.
     *
     * @return true if the type is STRING, false otherwise
     */
    public boolean isString()
    {
        return type == PkType.STRING;
    }

    /**
     * Generates split points for dividing the primary key range into multiple segments.
     * The split points exclude the minimum and maximum values.
     *
     * @param splitNum The number of segments to create (must be &ge; 2)
     * @return List of split point values, empty if splitNum &lt; 2 or data type not supported
     */
    public List<Object> genSplitPoint(int splitNum)
    {
        if (splitNum < 2) {
            return Collections.emptyList();
        }
        List<Object> result = new java.util.ArrayList<>();
        if (isLong()) {
            long min = Long.parseLong(this.min.toString());
            long max = Long.parseLong(this.max.toString());
            long step = (max - min) / splitNum;
            // exclude min and max
            for (long i = 1; i < splitNum; i++) {
                result.add(min + i * step);
            }
            return result;
        }
        else if (isFloat()) {
            return genFloatSplitPoint(splitNum);
        }
        return result;
    }

    /**
     * Generates split points specifically for floating-point primary keys.
     * Uses rounding to ensure integer step values for better distribution.
     *
     * @param splitNum The number of segments to create (must be >= 2)
     * @return List of split point values for floating-point ranges
     */
    public List<Object> genFloatSplitPoint(int splitNum)
    {
        if (splitNum < 2) {
            return Collections.emptyList();
        }
        List<Object> result = new java.util.ArrayList<>();
        double min = Double.parseDouble(this.min.toString());
        double max = Double.parseDouble(this.max.toString());
        if ((max - min) <= splitNum) {
            // the difference between min and max is less than splitNum
            return result;
        }
        double step = Math.round((max - min) / splitNum);
        // exclude min and max
        for (long i = 1; i < splitNum; i++) {
            result.add(min + i * step);
        }
        return result;
    }

    /**
     * Checks if the minimum and maximum values are the same.
     *
     * @return true if min equals max, false otherwise
     */
    public boolean isSameValue()
    {
        return Objects.equals(min, max);
    }
}