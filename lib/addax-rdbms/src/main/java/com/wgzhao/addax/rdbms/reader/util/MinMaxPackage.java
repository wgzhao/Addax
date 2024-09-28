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

import com.wgzhao.addax.common.base.Constant;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MinMaxPackage
{
    private Object min;
    private Object max;
    private Object type;

    public MinMaxPackage()
    {
        this.min = null;
        this.max = null;
        this.type = null;
    }

    public Object getMin()
    {
        return min;
    }

    public void setMin(Object min)
    {
        this.min = min;
    }

    public Object getMax()
    {
        return max;
    }

    public void setMax(Object max)
    {
        this.max = max;
    }

    public Object getType()
    {
        return type;
    }

    public void setType(Object type)
    {
        this.type = type;
    }

    public boolean isLong()
    {
        return type == Constant.PK_TYPE_LONG;
    }

    public boolean isFloat()
    {
        return type == Constant.PK_TYPE_FLOAT;
    }

    public boolean isNumeric() {
        return isLong() || isFloat();
    }

    public boolean isString()
    {
        return type == Constant.PK_TYPE_STRING;
    }

    public List<Object> genSplitPoint(int splitNum) {
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

    public List<Object> genFloatSplitPoint(int splitNum) {
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

    public boolean isSameValue() {
        return Objects.equals(min, max);
    }
}