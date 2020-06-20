package com.github.superzhc.livy;

import com.github.superzhc.spark.AbstractSparkSession;

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

    @Deprecated
    public <T extends AbstractSparkSession> Object wrapper(T obj){
        return SparkLivyProxy.newProxyInstance(client, obj);
    }

    public <T extends AbstractSparkSession> T cglib(T obj) {
        return SparkLivyCGLibProxy.newProxyInstance(client, obj);
    }

    public Integer getSessionId() {
        return sessionId;
    }
}
