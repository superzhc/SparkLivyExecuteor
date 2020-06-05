package com.github.superzhc.livy;

import org.apache.livy.Job;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Method;
import java.net.URI;

/**
 * 2020年06月01日 superz add
 */
public class SparkLivyClient
{
    private static final Logger logger = LoggerFactory.getLogger(SparkLivyClient.class);

    private static String _livyUrl;
    private static String _jars;

    static {
        _livyUrl = System.getProperty("livy.url");
        logger.debug("livy.url:[{}]", _livyUrl);
        _jars = System.getProperty("livy.job.jars");
        logger.debug("livy.job.jars:[{}]", _jars);
    }

    private LivyClient client;
    private Integer sessionId;

    public SparkLivyClient() {
        this(null, (String) null);
    }

    public SparkLivyClient(String livyUrl, String jars) {
        String __livyUrl;
        if (null != livyUrl && livyUrl.trim() != "")
            __livyUrl = livyUrl;
        else
            __livyUrl = _livyUrl;
        logger.debug("Livy的访问地址：" + __livyUrl);
        logger.info("创建LivyClient[{}]开始...", __livyUrl);
        this.client = createLivyClient(__livyUrl);
        logger.info("创建LivyClient[{}]成功！", __livyUrl);

        // 上传jar包
        String __jars;
        if (null != jars && jars.trim() != "")
            __jars = jars;
        else
            __jars = _jars;
        uploadJar(__jars);
    }

    /**
     * LivyServer中已存在session，建立重新的连接
     * 重新建立连接的，无需重新上传Jar包
     * @param sessionId
     */
    public SparkLivyClient(Integer sessionId) {
        this(null, sessionId);
    }

    /**
     * LivyServer中已存在session，建立重新的连接
     * 重新建立连接的，无需重新上传Jar包
     * @param livyUrl
     * @param sessionId
     */
    public SparkLivyClient(String livyUrl, Integer sessionId) {
        this.sessionId = sessionId;

        String __livyUrl;
        if (null != livyUrl && livyUrl.trim() != "")
            __livyUrl = livyUrl;
        else
            __livyUrl = _livyUrl;
        String url = __livyUrl + "/sessions/" + sessionId;
        logger.info("连接Livy[livyUrl={}]的Session[sessionId={}]开始...", __livyUrl, sessionId);
        this.client = createLivyClient(url);
        logger.info("连接Livy[livyUrl={}]的Session[sessionId={}]完成", __livyUrl, sessionId);
    }

    public Integer getSessionId() {
        if (null == sessionId) {
            try {
                Method method = this.client.getClass().getDeclaredMethod("getSessionId");
                method.setAccessible(true);
                sessionId = (Integer) method.invoke(this.client);
            }
            catch (Exception e) {
            }
        }
        return sessionId;
    }

    public void uploadJar(String path) {
        logger.debug("上传的jars地址：" + path);
        try {
            String[] arr = path.split(";");
            for (String jar : arr) {
                File file = new File(jar);
                if (!file.exists()) {
                    logger.error("文件[{}]不存在", file.getName());
                    continue;
                }

                if (file.isDirectory()) {
                    File[] childFiles = file.listFiles(new FileFilter()
                    {
                        @Override
                        public boolean accept(File pathname) {
                            return pathname.getName().endsWith(".jar");
                        }
                    });
                    for (File childFile : childFiles) {
                        logger.info("上传Jar包[{}]开始...", childFile.getName());
                        this.client.uploadJar(childFile).get();
                        logger.info("上传Jar包[{}]成功！", childFile.getName());
                    }
                }
                else {
                    logger.info("上传Jar包[{}]开始...", file.getName());
                    this.client.uploadJar(file).get();
                    logger.info("上传Jar包[{}]成功！", file.getName());
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("上传Jar包失败:" + e.getMessage());
        }
    }

    public <T> T submit(Job<T> job) {
        try {
            return client.submit(job).get();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("任务执行失败");
        }
    }

    private LivyClient createLivyClient(String url) {
        try {
            return new LivyClientBuilder().setURI(new URI(url)).build();
        }
        catch (Exception e) {
            throw new RuntimeException("创建LivyClient异常：" + e.getMessage());
        }
    }
}