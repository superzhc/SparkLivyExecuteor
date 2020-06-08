package com.github.superzhc.livy;

import com.github.superzhc.utils.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * 杀掉Session job
     * @param sessionId
     */
    public Integer delete(Integer sessionId) {
        HttpRequest request = HttpRequest.delete(fullUrl("/sessions/%d", sessionId));
        return request.code();
    }

    /**
     * 获取日志信息
     * @param sessionId
     * @return
     */
    public String log(Integer sessionId) {
        HttpRequest request = HttpRequest.get(fullUrl("/sessions/%d/log", sessionId));
        return request.body();
    }

    private String fullUrl(String url, Object... params) {
        return String.format("%s" + url, this.livyUrl, params);
    }
}
