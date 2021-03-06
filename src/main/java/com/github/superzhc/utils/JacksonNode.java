package com.github.superzhc.utils;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

/**
 * 2020年06月23日 superz add
 */
public class JacksonNode implements Serializable
{
    private JsonNode node;

    public JacksonNode(JsonNode node) {
        this.node = node;
    }

    public JacksonNode get(String... fieldNames) {
        JsonNode node1 = deepGet(fieldNames);
        return new JacksonNode(node1);
    }

    public Integer getInteger(String... fieldNames) {
        JsonNode node1 = deepGet(fieldNames);
        return null == node1 ? null : node1.asInt();
    }

    public Double getDouble(String... fieldNames) {
        JsonNode node1 = deepGet(fieldNames);
        return null == node1 ? null : node1.asDouble();
    }

    public String getString(String... fieldNames) {
        JsonNode node1 = deepGet(fieldNames);
        return null == node1 ? null : node1.asText();
    }

    private JsonNode deepGet(String... fieldNames) {
        if (null == fieldNames || fieldNames.length == 0)
            return node;

        JsonNode node1 = node;
        for (String fieldName : fieldNames) {
            if (null == node1)// 防止报空指针
                return null;
            node1 = node1.get(fieldName);
        }
        return node1;
    }

    public JsonNode origin() {
        return node;
    }
}
