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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.spi.ErrorCode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/**
 * Configuration provides multi-level JSON configuration information lossless storage <br>
 * Instance code: <br>
 * Get the configuration information of the job <br>
 * <pre>
 * Configuration configuration = Configuration.from(new File("Config.json")); <br>
 * String jobContainerClass = configuration.getString("core.container.job.class"); <br>
 * Set multi-level List <br>
 * configuration.set("job.reader.parameter.jdbcUrl", Arrays.asList(new String[] {"jdbc", "jdbc"})); <br>
 * Merge Configuration: <br>
 * configuration.merge(another);
 * </pre>
 * There are two better implementations of Configuration <br>
 * The first is to flatten all the keys in the JSON configuration information, use the cascade of a.b.c as the key of the Map, and use a Map to save the information <br>
 * The second is to directly use the structured tree structure to save the JSON object <br>
 * Currently use the second implementation method, the problem with the first method is: <br>
 * 1. Inserting new objects is relatively difficult to handle, for example, a.b.c="foo", at this time, if you need to insert a="foo", that is,
 * all types of the first layer under the root need to be discarded, and "foo" is used as the value, the first type The way to use a string to
 * represent the key is difficult to handle such problems. <br>
 * 2. Return the tree structure, for example, a.b.c.d = "foo", if you return all elements under "a", it is actually a Map that needs to be merged and processed <br>
 * 3. Output JSON, convert the above objects to JSON, convert the multi-level key of the above Map to a tree structure, and output it as JSON <br>
 */
public class Configuration
{

    private Object root;

    private Configuration(String json)
    {
        try {
            this.root = JSON.parse(json);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("The configuration is incorrect. The configuration you provided is " + "not in valid JSON format: %s.", e.getMessage()));
        }
    }

    public static Configuration newDefault()
    {
        return Configuration.from("{}");
    }

    public static Configuration from(String json)
    {
        json = StrUtil.replaceVariable(json);
        checkJSON(json);

        try {
            return new Configuration(json);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, e);
        }
    }

    public static Configuration from(File file)
    {
        try {
            return Configuration.from(IOUtils.toString(new FileInputStream(file), StandardCharsets.UTF_8));
        }
        catch (FileNotFoundException e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("No such file: %s", file.getAbsolutePath()));
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("Failed to read the configuration [%s], reason: %s.", file.getAbsolutePath(), e));
        }
    }

    /**
     * load configuration from json InputStream
     *
     * @param is InputStream
     * @return {@link Configuration}
     */
    public static Configuration from(InputStream is)
    {
        try {
            return Configuration.from(IOUtils.toString(is, StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("Failed to read the configuration reason: %s.", e));
        }
    }

    public static Configuration from(final Map<String, Object> object)
    {
        return Configuration.from(Configuration.toJSONString(object));
    }

    public static Configuration from(final List<Object> object)
    {
        return Configuration.from(Configuration.toJSONString(object));
    }

    private static void checkJSON(String json)
    {
        if (StringUtils.isBlank(json)) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, "The configure file is empty.");
        }
    }

    private static String toJSONString(Object object)
    {
        return JSON.toJSONString(object);
    }

    public String getNecessaryValue(String key, ErrorCode errorCode)
    {
        String value = this.getString(key, null);
        if (StringUtils.isBlank(value)) {
            throw AddaxException.asAddaxException(errorCode, String.format("The required item '%s' is not found", key));
        }
        return value;
    }

    public String getNecessaryValue(String key)
    {
        String value = this.getString(key, null);
        if (StringUtils.isBlank(value)) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, String.format("The required item %s is not found", key));
        }
        return value;
    }

    public void getNecessaryValues(String... keys)
    {
        for (String key : keys) {
            if (StringUtils.isBlank(this.getString(key, null))) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, String.format("The required item %s is not found", key));
            }
        }
    }

    public String getUnnecessaryValue(String key, String defaultValue)
    {
        String value = this.getString(key, defaultValue);
        if (StringUtils.isBlank(value)) {
            value = defaultValue;
        }
        return value;
    }

    /**
     * find object by path
     * Locate the specific object based on the JSON path provided by the user.
     * NOTE: Currently only supports Map and List index addressing, for example:
     * For the following JSON
     * <p>
     * {"a": {"b": {"c": [0,1,2,3]}}}
     * </p>
     * config.get("") returns the entire Map <br>
     * config.get("a") returns the entire Map under a <br>
     * config.get("a.b.c") returns the array List corresponding to c <br>
     * config.get("a.b.c[0]") returns the number 0
     *
     * @param path String the JSON path to query
     * @return Java representation of the JSON object, returns null if the path does not exist or the object does not exist.
     */
    public Object get(String path)
    {
        this.checkPath(path);
        try {
            return this.findObject(path);
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * get the subset of Configuration by user-specified json path
     *
     * @param path String json path
     * @return Configuration object, if the path or object obtained by path does not exist, return null
     */
    public Configuration getConfiguration(String path)
    {
        Object object = this.get(path);
        if (null == object) {
            return null;
        }

        return Configuration.from(Configuration.toJSONString(object));
    }

    /**
     * get the string value by user-specified json path
     *
     * @param path String json path
     * @return String value, if the path or object obtained by path does not exist, return null
     */
    public String getString(String path)
    {
        Object string = this.get(path);
        if (null == string) {
            return null;
        }
        return String.valueOf(string);
    }

    /**
     * get the string value by user-specified json path
     *
     * @param path String json path
     * @param defaultValue String default value
     * @return String value, if the path or object obtained by path does not exist, return the default value
     */
    public String getString(String path, String defaultValue)
    {
        String result = this.getString(path);

        if (null == result) {
            return defaultValue;
        }

        return result;
    }

    /**
     * get the boolean value by user-specified json path
     *
     * @param path String json path
     * @return Boolean value, if the path or object obtained by path does not exist, return null
     */
    public Character getChar(String path)
    {
        String result = this.getString(path);
        if (null == result) {
            return null;
        }

        try {
            return CharUtils.toChar(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("Illegal configuration, cannot be converted to string for [%s], reason: %s.", path, e.getMessage()));
        }
    }

    /**
     * get the boolean value by user-specified json path
     *
     * @param path String json path
     * @param defaultValue String default value
     * @return Boolean value, if the path or object obtained by path does not exist, return the default value
     */
    public Character getChar(String path, char defaultValue)
    {
        Character result = this.getChar(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * get the boolean value by user-specified json path
     *
     * @param path String json path
     * @return Boolean value, if the path or object obtained by path does not exist, return null
     */
    public Boolean getBool(String path)
    {
        String result = this.getString(path);

        if (null == result) {
            return null;
        }
        else if ("true".equalsIgnoreCase(result)) {
            return Boolean.TRUE;
        }
        else if ("false".equalsIgnoreCase(result)) {
            return Boolean.FALSE;
        }
        else {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("Illegal configuration, the value [%s] of [%s] cannot be converted to bool type.", path, result));
        }
    }

    /**
     * get the boolean value by user-specified json path
     *
     * @param path String json path
     * @param defaultValue String default value
     * @return Boolean value, if the path or object obtained by path does not exist, return the default value
     */
    public Boolean getBool(String path, boolean defaultValue)
    {
        Boolean result = this.getBool(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * get the integer value by user-specified json path
     *
     * @param path String json path
     * @return Integer value, if the path or object obtained by path does not exist, return null
     */
    public Integer getInt(String path)
    {
        String result = this.getString(path);
        if (null == result) {
            return null;
        }

        try {
            return Integer.valueOf(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("Illegal configuration, the value [%s] of [%s] is expected integer type.", path, e.getMessage()));
        }
    }

    /**
     * get the integer value by user-specified json path
     *
     * @param path String json path
     * @param defaultValue String default value
     * @return Integer value, if the path or object obtained by path does not exist, return the default value
     */
    public Integer getInt(String path, int defaultValue)
    {
        Integer object = this.getInt(path);
        if (null == object) {
            return defaultValue;
        }
        return object;
    }

    /**
     * get the long value by user-specified json path
     *
     * @param path String json path
     * @return Long value, if the path or object obtained by path does not exist, return null
     */
    public Long getLong(String path)
    {
        String result = this.getString(path);
        if (StringUtils.isBlank(result)) {
            return null;
        }

        try {
            return Long.valueOf(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("Illegal configuration, the value [%s] of [%s] is expected integer/long type.", path, e.getMessage()));
        }
    }

    /**
     * get the long value by user-specified json path
     *
     * @param path String json path
     * @param defaultValue String default value
     * @return Long value, if the path or object obtained by path does not exist, return the default value
     */
    public Long getLong(String path, long defaultValue)
    {
        Long result = this.getLong(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * get the double value by user-specified json path
     *
     * @param path String json path
     * @return Double value, if the path or object obtained by path does not exist, return null
     */
    public Double getDouble(String path)
    {
        String result = this.getString(path);
        if (StringUtils.isBlank(result)) {
            return null;
        }

        try {
            return Double.valueOf(result);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.CONFIG_ERROR, String.format("Illegal configuration, the value [%s] of [%s] is expected float type.", path, e.getMessage()));
        }
    }

    /**
     * get the double value by user-specified json path
     *
     * @param path String json path
     * @param defaultValue String default value
     * @return Double value, if the path or object obtained by path does not exist, return the default value
     */
    public Double getDouble(String path, double defaultValue)
    {
        Double result = this.getDouble(path);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * get the list value by user-specified json path
     *
     * @param path String json path
     * @return List value, if the path or object obtained by path does not exist, return null
     */
    public List<Object> getList(String path)
    {
        return this.get(path, List.class);
    }

    /**
     * get the list value by user-specified json path
     *
     * @param path String json path
     * @param clazz Class type
     * @param <T> type
     * @return List value, if the path or object obtained by path does not exist, return null
     */
    public <T> List<T> getList(String path, Class<T> clazz)
    {
        List<Object> object = this.get(path, List.class);
        if (null == object) {
            return new ArrayList<>();
        }

        List<T> result = new ArrayList<>();

        List<Object> origin;
        try {
            origin = object;
        }
        catch (ClassCastException e) {
            origin = new ArrayList<>();
            origin.add(String.valueOf(object));
        }

        for (Object each : origin) {
            result.add(clazz.cast(each));
        }

        return result;
    }

    /**
     * get the list value by user-specified json path
     *
     * @param path String json path
     * @param defaultList List default value
     * @return List value, if the path or object obtained by path does not exist, return the default value
     */
    public List<Object> getList(String path, List<Object> defaultList)
    {
        List<Object> object = this.getList(path);
        if (null == object) {
            return defaultList;
        }
        return object;
    }

    /**
     * get the list value by user-specified json path
     *
     * @param path String json path
     * @param defaultList List default value
     * @param t Class type
     * @param <T> type
     * @return List value, if the path or object obtained by path does not exist, return the default value
     */
    public <T> List<T> getList(String path, List<T> defaultList, Class<T> t)
    {
        List<T> list = this.getList(path, t);
        if (null == list) {
            return defaultList;
        }
        return list;
    }

    /**
     * get the map value by user-specified json path
     *
     * @param path String json path
     * @return Map value, if the path or object obtained by path does not exist, return null
     */
    public List<Configuration> getListConfiguration(String path)
    {
        List<Object> lists = getList(path);
        if (lists == null) {
            return new ArrayList<>();
        }

        List<Configuration> result = new ArrayList<>();
        for (Object object : lists) {
            result.add(Configuration.from(Configuration.toJSONString(object)));
        }
        return result;
    }

    /**
     * get the map value by user-specified json path
     *
     * @param path String json path
     * @return Map value, if the path or object obtained by path does not exist, return null
     */
    public Map<String, Object> getMap(String path)
    {
        return this.get(path, Map.class);
    }

    /**
     * get the map value by user-specified json path
     *
     * @param path String json path
     * @param defaultMap Map default value
     * @return Map value, if the path or object obtained by path does not exist, return the default value
     */
    public Map<String, Object> getMap(String path, Map<String, Object> defaultMap)
    {
        Map<String, Object> object = this.getMap(path);
        if (null == object) {
            return defaultMap;
        }
        return object;
    }

    /**
     * Locate the specific object based on the JSON path provided by the user and convert it to the specified type.
     * NOTE: Currently only supports Map and List index addressing, for example:
     * For the following JSON:
     * <p>
     * {"a": {"b": {"c": [0,1,2,3]}}}
     * </p>
     * config.get("") returns the entire Map <br>
     * config.get("a") returns the entire Map under "a" <br>
     * config.get("a.b.c") returns the List corresponding to "c" <br>
     * config.get("a.b.c[0]") returns the number 0
     *
     * @param path The JSON path
     * @param clazz The class type to convert to
     * @param <T> The type
     * @return The Java representation of the JSON object, throws an exception if the conversion fails
     */
    public <T> T get(String path, Class<T> clazz)
    {
        this.checkPath(path);
        Object object = this.get(path);
        if (object == null) {
            return null;
        }
        return clazz.cast(object);
    }

    public String beautify()
    {
        return JSON.toJSONString(this.getInternal(), JSONWriter.Feature.PrettyFormat);
    }

    /**
     * Inserts the specified object at the given JSON path and returns the previously existing object (if any).
     * Currently, only dot notation and array index addressing are supported, for example:
     * <p>
     * config.set("a.b.c[3]", object);
     * </p>
     * There are no restrictions on the object being inserted, but please ensure that the object is a simple object.
     * Avoid using custom objects, as this may lead to undefined behavior during subsequent JSON serialization.
     *
     * @param path JSON path
     * @param object Object to be inserted
     * @return Java representation of the JSON object
     */
    public Object set(String path, Object object)
    {
        checkPath(path);

        Object result = this.get(path);

        setObject(path, extractConfiguration(object));

        return result;
    }

    /**
     * get all keys of the configuration
     * for example:
     * <p>
     * {"a": {"b": {"c": [0,1,2,3]}}, "x": "y"}
     * </p>
     * the keys includes: a.b.c[0],a.b.c[1],a.b.c[2],a.b.c[3],x
     *
     * @return set of string
     */
    public Set<String> getKeys()
    {
        Set<String> collect = new HashSet<>();
        this.getKeysRecursive(this.getInternal(), "", collect);
        return collect;
    }

    /**
     * Deletes the value corresponding to the path. If the path does not exist, an exception will be thrown.
     *
     * @param path String json path
     * @return Object the object found, or null if it does not exist
     */
    public Object remove(String path)
    {
        Object result = this.get(path);
        if (null == result) {
            throw AddaxException.asAddaxException(ErrorCode.RUNTIME_ERROR, "Illegal configuration, the key '" + path + "' does not exists.");
        }

        this.set(path, null);
        return result;
    }

    /**
     * Merge another Configuration and modify the KV configuration of the two when they conflict
     *
     * @param another Configuration the third-party Configuration to be merged
     * @param updateWhenConflict boolean when there is a KV conflict between the two merged parties, choose to update the current KV, or ignore the KV
     */
    public void merge(Configuration another, boolean updateWhenConflict)
    {
        Set<String> keys = another.getKeys();

        // if updateWhenConflict is true, update all keys in another
        // if updateWhenConflict is false, only update the key that another has but the current does not
        keys.forEach(key -> {
            if (updateWhenConflict) {
                this.set(key, another.get(key));
                return;
            }
            boolean isCurrentExists = this.get(key) != null;
            if (isCurrentExists) {
                return;
            }
            this.set(key, another.get(key));
        });
    }

    @Override
    public String toString()
    {
        return this.toJSON();
    }

    public String toJSON()
    {
        return Configuration.toJSONString(this.getInternal());
    }

    @Override
    public Configuration clone()
    {
        return Configuration.from(Configuration.toJSONString(this.getInternal()));
    }

    void getKeysRecursive(Object current, String path, Set<String> collect)
    {
        boolean isRegularElement = !(current instanceof Map || current instanceof List);
        if (isRegularElement) {
            collect.add(path);
            return;
        }

        boolean isMap = current instanceof Map;
        if (isMap) {
            Map<String, Object> mapping = ((Map<String, Object>) current);
            mapping.keySet().forEach(key -> {
                if (StringUtils.isBlank(path)) {
                    getKeysRecursive(mapping.get(key), key.trim(), collect);
                }
                else {
                    getKeysRecursive(mapping.get(key), path + "." + key.trim(), collect);
                }
            });
            return;
        }

        List<Object> lists = (List<Object>) current;
        for (int i = 0; i < lists.size(); i++) {
            getKeysRecursive(lists.get(i), path + String.format("[%d]", i), collect);
        }
    }

    public Object getInternal()
    {
        return this.root;
    }

    private void setObject(String path, Object object)
    {
        Object newRoot = setObjectRecursive(this.root, split2List(path), 0, object);

        if (isSuitForRoot(newRoot)) {
            this.root = newRoot;
            return;
        }

        throw AddaxException.asAddaxException(ErrorCode.RUNTIME_ERROR, String.format("Illegal value, cannot set value [%s] for key [%s]", ToStringBuilder.reflectionToString(object), path));
    }

    private Object extractConfiguration(Object object)
    {
        if (object instanceof Configuration) {
            return extractFromConfiguration(object);
        }

        if (object instanceof List) {
            List<Object> result = new ArrayList<>();
            for (Object each : (List<Object>) object) {
                result.add(extractFromConfiguration(each));
            }
            return result;
        }

        if (object instanceof Map) {
            Map<String, Object> result = new HashMap<>();
            for (String key : ((Map<String, Object>) object).keySet()) {
                result.put(key, extractFromConfiguration(((Map<String, Object>) object).get(key)));
            }
            return result;
        }

        return object;
    }

    private Object extractFromConfiguration(Object object)
    {
        if (object instanceof Configuration) {
            return ((Configuration) object).getInternal();
        }

        return object;
    }

    Object buildObject(List<String> paths, Object object)
    {
        if (null == paths) {
            throw AddaxException.asAddaxException(ErrorCode.RUNTIME_ERROR, "The Path cannot be null");
        }

        if (1 == paths.size() && StringUtils.isBlank(paths.get(0))) {
            return object;
        }

        Object child = object;
        for (int i = paths.size() - 1; i >= 0; i--) {
            String path = paths.get(i);

            if (isPathMap(path)) {
                Map<String, Object> mapping = new HashMap<>();
                mapping.put(path, child);
                child = mapping;
                continue;
            }

            if (isPathList(path)) {
                List<Object> lists = new ArrayList<>(this.getIndex(path) + 1);
                expand(lists, this.getIndex(path) + 1);
                lists.set(this.getIndex(path), child);
                child = lists;
                continue;
            }

            throw AddaxException.asAddaxException(ErrorCode.RUNTIME_ERROR, String.format("the value [%s] of [%s] is illegal.", StringUtils.join(paths, "."), path));
        }

        return child;
    }

    Object setObjectRecursive(Object current, List<String> paths, int index, Object value)
    {
        // if the index is out of the path, we return the value as the leaf node
        boolean isLastIndex = index == paths.size();
        if (isLastIndex) {
            return value;
        }

        String path = paths.get(index).trim();
        boolean isNeedMap = isPathMap(path);
        if (isNeedMap) {
            Map<String, Object> mapping;

            // the current is not a map, so we replace it with a map and return the new map object
            boolean isCurrentMap = current instanceof Map;
            if (!isCurrentMap) {
                mapping = new HashMap<>();
                mapping.put(path, buildObject(paths.subList(index + 1, paths.size()), value));
                return mapping;
            }

            // the current is a map, but the key does not exist, so we need to insert the object and return the map
            mapping = ((Map<String, Object>) current);
            boolean hasSameKey = mapping.containsKey(path);
            if (!hasSameKey) {
                mapping.put(path, buildObject(paths.subList(index + 1, paths.size()), value));
                return mapping;
            }

            // 当前是map，而且还竟然存在这个值，好吧，继续递归遍历
            current = mapping.get(path);
            mapping.put(path, setObjectRecursive(current, paths, index + 1, value));
            return mapping;
        }

        boolean isNeedList = isPathList(path);
        if (isNeedList) {
            List<Object> lists;
            int listIndexer = getIndex(path);

            // 当前是list，直接新建并返回即可
            boolean isCurrentList = current instanceof List;
            if (!isCurrentList) {
                lists = expand(new ArrayList<>(), listIndexer + 1);
                lists.set(listIndexer, buildObject(paths.subList(index + 1, paths.size()), value));
                return lists;
            }

            // the current is a list, but the index does not exist, so we need to insert the object and return the list
            lists = expand((List<Object>) current, listIndexer + 1);

            boolean hasSameIndex = lists.get(listIndexer) != null;
            if (!hasSameIndex) {
                lists.set(listIndexer, buildObject(paths.subList(index + 1, paths.size()), value));
                return lists;
            }

            // the current is a list, and the index exists, so we need to continue to find the value
            current = lists.get(listIndexer);
            lists.set(listIndexer, setObjectRecursive(current, paths, index + 1, value));
            return lists;
        }

        throw AddaxException.asAddaxException(ErrorCode.RUNTIME_ERROR, "System internal error.");
    }

    private Object findObject(String path)
    {
        boolean isRootQuery = StringUtils.isBlank(path);
        if (isRootQuery) {
            return this.root;
        }

        Object target = this.root;

        for (String each : split2List(path)) {
            if (isPathMap(each)) {
                target = findObjectInMap(target, each);
            }
            else {
                target = findObjectInList(target, each);
            }
        }

        return target;
    }

    private Object findObjectInMap(Object target, String index)
    {
        boolean isMap = (target instanceof Map);
        if (!isMap) {
            throw new IllegalArgumentException(String.format("The item [%s] requires a Map object in json format, but the actual type is [%s].", index, target.getClass()));
        }

        Object result = ((Map<String, Object>) target).get(index);
        if (null == result) {
            throw new IllegalArgumentException("The value of item '" + index + "'is null.");
        }

        return result;
    }

    private Object findObjectInList(Object target, String each)
    {
        boolean isList = (target instanceof List);
        if (!isList) {
            throw new IllegalArgumentException(String.format("The item [%s] requires a Map object in json format, but the actual type is [%s].", each, target.getClass()));
        }

        String index = each.replace("[", "").replace("]", "");
        if (!StringUtils.isNumeric(index)) {
            throw new IllegalArgumentException(String.format("The list subscript must be a numeric type, but the actual type is [%s].", index));
        }

        return ((List<Object>) target).get(Integer.parseInt(index));
    }

    private List<Object> expand(List<Object> list, int size)
    {
        int expand = size - list.size();
        while (expand-- > 0) {
            list.add(null);
        }
        return list;
    }

    private boolean isPathList(String path)
    {
        return path.contains("[") && path.contains("]");
    }

    private boolean isPathMap(String path)
    {
        return StringUtils.isNotBlank(path) && !isPathList(path);
    }

    private int getIndex(String index)
    {
        return Integer.parseInt(index.replace("[", "").replace("]", ""));
    }

    private boolean isSuitForRoot(Object object)
    {
        return (object instanceof List || object instanceof Map);
    }

    private String split(String path)
    {
        return StringUtils.replace(path, "[", ".[");
    }

    private List<String> split2List(String path)
    {
        return Arrays.asList(StringUtils.split(split(path), "."));
    }

    private void checkPath(String path)
    {
        if (null == path) {
            throw new IllegalArgumentException("System internal error.");
        }

        for (String each : StringUtils.split(path, ".")) {
            if (StringUtils.isBlank(each)) {
                throw new IllegalArgumentException("The item '" + path + "' is invalid, Blank characters should not  appear here.");
            }
        }
    }
}
