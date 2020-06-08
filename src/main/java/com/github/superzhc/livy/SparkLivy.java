package com.github.superzhc.livy;

/**
 * 2020年06月05日 superz add
 */
public class SparkLivy
{
    private SparkLivyClient client;
    private final Integer sessionId;

    public SparkLivy() {
        this.client = new SparkLivyClient();
        sessionId=this.client.getSessionId();
    }

    public SparkLivy(Integer sessionId) {
        this.sessionId=sessionId;
        this.client = new SparkLivyClient(sessionId);
    }

    public <T extends AbstractSparkSession> Object wrapper(T obj){
        return SparkLivyProxy.newProxyInstance(client, obj);
    }

    public Integer getSessionId() {
        return sessionId;
    }
}
