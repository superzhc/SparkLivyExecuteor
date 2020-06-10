package com.github.superzhc.livy;

import com.alibaba.fastjson.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.superzhc.utils.HttpRequest;

/**
 * 2020年06月08日 superz add
 */
public class SparkLivyRestClient
{
    private static final Logger logger = LoggerFactory.getLogger(SparkLivyRestClient.class);

    private static String _livyUrl;

    static {
        _livyUrl = System.getProperty("livy.url");
        logger.debug("livy.url:[{}]", _livyUrl);
    }

    private String livyUrl;

    public SparkLivyRestClient() {
        this(null);
    }

    public SparkLivyRestClient(String livyUrl) {
        if (null != livyUrl)
            this.livyUrl = livyUrl;
        else
            this.livyUrl = _livyUrl;
    }

    public String sessions() {
        HttpRequest request = HttpRequest.get(fullUrl("/sessions"));
        return request.body();
    }

    private String fullUrl(String path, Object... params) {
        return String.format(this.livyUrl + path, params);
    }

    public Session Session(Integer sessionId) {
        return new Session(livyUrl, sessionId);
    }

    public Statement Statement(Integer sessionId, Integer statementId) {
        return new Statement(livyUrl, sessionId, statementId);
    }

    public static class Session
    {
        private String livyUrl;
        private Integer sessionId;

        public Session(String livyUrl, Integer sessionId) {
            this.livyUrl = livyUrl;
            this.sessionId = sessionId;
        }

        public String get() {
            HttpRequest request = HttpRequest.get(fullUrl(""));
            return request.body();
        }

        public String driverLogUrl() {
            String ret = get();
            return JSON.parseObject(ret).getJSONObject("appInfo").getString("driverLogUrl");
        }

        public String sparkUI() {
            String ret = get();
            return JSON.parseObject(ret).getJSONObject("appInfo").getString("sparkUiUrl");
        }

        public String state() {
            HttpRequest request = HttpRequest.get(fullUrl("/state"));
            return request.body();
        }

        /**
         * 杀掉Session job
         */
        public Integer delete() {
            HttpRequest request = HttpRequest.delete(fullUrl(""));
            return request.code();
        }

        /**
         * 获取日志信息
         * @return
         */
        public String log() {
            StringBuilder sb = new StringBuilder();

            HttpRequest request = HttpRequest.get(fullUrl("/log"));
            String ret = request.body();
            logger.debug("获取Livy[sessionId={}]的日志信息：{}", sessionId, ret);
            JSONArray logs = JSON.parseObject(ret).getJSONArray("log");
            for (int i = 0, len = logs.size(); i < len; i++) {
                sb.append(logs.get(i)).append("\n");
            }
            return sb.toString();
        }

        public String statements() {
            HttpRequest request = HttpRequest.get(fullUrl("/statements"));
            return request.body();
        }

        public Integer execute(String code) {
            return execute(code, "spark");
        }

        public Integer execute(String code, String kind) {
            logger.debug("执行代码片段：{}", code);
            JSONObject data = new JSONObject();
            data.put("kind", kind);
            data.put("code", code);
            HttpRequest request = HttpRequest.post(fullUrl("/statements")).contentType("application/json");
            request.send(JSON.toJSONBytes(data));
            String ret = request.body();
            logger.debug("执行代码片段的返回结果：{}", ret);
            if (null == ret || "".equals(ret))
                return -1;
            return JSON.parseObject(ret).getInteger("id");
        }

        private String fullUrl(String path) {
            String prefix = String.format("%s/sessions/%d", livyUrl, sessionId);
            return prefix + path;
        }
    }

    public static class Statement
    {
        private String livyUrl;
        private Integer sessionId;
        private Integer statementId;

        public Statement(String livyUrl, Integer sessionId, Integer statementId) {
            this.livyUrl = livyUrl;
            this.sessionId = sessionId;
            this.statementId = statementId;
        }

        public String get() {
            StringBuilder sb = new StringBuilder();
            double progress = 0.0;
            while (progress < 1.0) {
                HttpRequest request = HttpRequest.get(fullUrl(""));
                String ret = request.body();
                logger.debug("Livy[sessionId={},statementId={}]的代码片段执行返回信息：{}", sessionId, statementId, ret);
                JSONObject obj = JSON.parseObject(ret);
                String state = obj.getString("state");
                if ("waiting".equals(state))
                    continue;
                progress = obj.getDouble("progress");
                String output = obj.getString("output");
                if (null == output || "".equals(output) || "null".equals(output))
                    continue;
                sb.append(output);
            }
            return sb.toString();
        }

        public String getAsync() {
            // TODO
            throw new RuntimeException("TODO！！！");
        }

        public String cancel() {
            HttpRequest request = HttpRequest.post(fullUrl("/cancel"));
            return request.body();
        }

        private String fullUrl(String path) {
            String prefix = String.format("%s/sessions/%d/statements/%d", livyUrl, sessionId, statementId);
            return prefix + path;
        }
    }
}
