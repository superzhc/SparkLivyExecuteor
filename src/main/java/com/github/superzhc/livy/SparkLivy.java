package com.github.superzhc.livy;

/**
 * 2020年06月05日 superz add
 */
public class SparkLivy
{
    private SparkLivyClient client;
    private Integer sessionId;

    public SparkLivy() {
        this.client = new SparkLivyClient();
        sessionId=this.client.getSessionId();
    }

    public SparkLivy(Integer sessionId) {
        this.sessionId=sessionId;
        this.client = new SparkLivyClient(sessionId);
    }

    public SparkLivy(SparkLivyClient client){
        this.client=client;
        this.sessionId=client.getSessionId();
    }

    public <T extends AbstractSparkSession> Object wrapper(T obj){
        return SparkLivyProxy.newProxyInstance(client, obj);
    }

    public Integer getSessionId() {
        return sessionId;
    }
}
