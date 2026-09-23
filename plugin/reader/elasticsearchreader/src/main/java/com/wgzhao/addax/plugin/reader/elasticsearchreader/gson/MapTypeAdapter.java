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

package com.wgzhao.addax.plugin.reader.elasticsearchreader.gson;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.internal.bind.ObjectTypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kesc
 * @since 2020-10-13 16:09
 */
public class MapTypeAdapter
        extends TypeAdapter<Object>
{
    /** Factory. */
    public static final TypeAdapterFactory FACTORY = new TypeAdapterFactory()
    {
        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type)
        {
            if (type.getRawType() == Map.class) {
                return (TypeAdapter<T>) new MapTypeAdapter(gson);
            }
            return null;
        }
    };

    private final Gson gson;

    MapTypeAdapter(Gson gson)
    {
        this.gson = gson;
    }

    @Override
    public Object read(JsonReader in)
            throws IOException
    {
        return switch (in.peek()) {
            case BEGIN_ARRAY -> readArray(in);
            case BEGIN_OBJECT -> readObject(in);
            case STRING -> in.nextString();
            case NUMBER -> readNumber(in.nextString());
            case BOOLEAN -> in.nextBoolean();
            case NULL -> {
                in.nextNull();
                yield null;
            }
            default -> throw new IllegalStateException("unexpected token");
        };
    }

    private List<Object> readArray(JsonReader in)
            throws IOException
    {
        List<Object> list = new ArrayList<>();
        in.beginArray();
        while (in.hasNext()) {
            list.add(read(in));
        }
        in.endArray();
        return list;
    }

    private Map<String, Object> readObject(JsonReader in)
            throws IOException
    {
        Map<String, Object> map = new LinkedTreeMap<>();
        in.beginObject();
        while (in.hasNext()) {
            map.put(in.nextName(), read(in));
        }
        in.endObject();
        return map;
    }

    /**
     * Numbers are classified as integral or floating point. Elasticsearch keeps the original
     * text of a document, so a value can be an integer beyond the range of a long (a field
     * mapped as double or scaled_float, or one that is not mapped at all); such a value is
     * kept as a BigDecimal instead of failing the whole read with a NumberFormatException.
     */
    private static Object readNumber(String numberStr)
    {
        if (numberStr.indexOf('.') >= 0 || numberStr.indexOf('e') >= 0 || numberStr.indexOf('E') >= 0) {
            return Double.parseDouble(numberStr);
        }
        try {
            long value = Long.parseLong(numberStr);
            // both bounds matter: casting a long below Integer.MIN_VALUE truncates silently
            if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                return (int) value;
            }
            return value;
        }
        catch (NumberFormatException e) {
            return new BigDecimal(numberStr);
        }
    }

    @Override
    public void write(JsonWriter out, Object value)
            throws IOException
    {
        if (value == null) {
            out.nullValue();
            return;
        }

        TypeAdapter<Object> typeAdapter = gson.getAdapter((Class<Object>) value.getClass());
        if (typeAdapter instanceof ObjectTypeAdapter) {
            out.beginObject();
            out.endObject();
            return;
        }

        typeAdapter.write(out, value);
    }
}