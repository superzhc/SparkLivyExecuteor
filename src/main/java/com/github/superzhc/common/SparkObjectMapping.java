package com.github.superzhc.common;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 2020年06月09日 superz add
 */
public abstract class SparkObjectMapping<T>
{
    private Map<String, T> map;

    public SparkObjectMapping() {
        map = new ConcurrentHashMap<>();
    }

    public T get(String key) {
        return map.get(key);
    }

    public String put(String key, T obj) {
        map.put(key, obj);
        return key;
    }

    public String set(T obj) {
        String uuid = UUID.randomUUID().toString();
        return put(uuid, obj);
    }

    public boolean contain(String key) {
        return map.containsKey(key);
    }
}
