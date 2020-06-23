package com.github.superzhc.livy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.superzhc.utils.HttpRequest;
import com.github.superzhc.utils.JacksonNode;
import com.github.superzhc.utils.JacksonUtils;

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
            try {
                return JacksonUtils.convert(ret).getString("appInfo", "driverLogUrl");
            }
            catch (Exception ex) {
                logger.error("解析json失败", ex);
            }
            return null;
        }

        public String sparkUI() {
            String ret = get();
            try {
                return JacksonUtils.convert(ret).getString("appInfo", "sparkUiUrl");
            }
            catch (Exception ex) {
                logger.error("解析json失败", ex);
            }
            return null;
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
            JsonNode logs=JacksonUtils.convert(ret).get("log").origin();
            for (int i = 0, len = logs.size(); i < len; i++) {
                sb.append(logs.get(i).asText()).append("\n");
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
            Map<String,String> data = new HashMap<>();
            data.put("kind", kind);
            data.put("code", code);
            HttpRequest request = HttpRequest.post(fullUrl("/statements")).contentType("application/json");
            request.send(JacksonUtils.toJavaString(data));
            String ret = request.body();
            logger.debug("执行代码片段的返回结果：{}", ret);
            if (null == ret || "".equals(ret))
                return -1;
            return JacksonUtils.convert(ret).getInteger("id");
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
                JacksonNode obj=JacksonUtils.convert(ret);
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

        /**
         * 异步获取数据
         * @return
         */
        public Future<String> getAsync() {
            Callable<String> callable = new Callable<String>()
            {
                @Override
                public String call() throws Exception {
                    return get();
                }
            };
            Future<String> future = new FutureTask<>(callable);
            return future;
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
