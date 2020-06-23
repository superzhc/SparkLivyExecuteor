package com.github.superzhc.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * 2020年06月23日 superz add
 */
public class JacksonUtils
{
    public static JacksonNode convert(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            // 解析器支持解析单引号
            mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
            // 解析器支持解析结束符
            mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
            return new JacksonNode(mapper.readTree(json));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map convert2Map(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            // 解析器支持解析单引号
            mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
            // 解析器支持解析结束符
            mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
            return mapper.readValue(json, Map.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJavaString(Object obj) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(obj);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String pretty(Object obj) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
