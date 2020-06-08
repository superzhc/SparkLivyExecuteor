package com.github.superzhc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.superzhc.dataframe.SparkDataFrame;
import com.github.superzhc.dataframe.SparkDataFrameImpl;
import com.github.superzhc.livy.SparkLivyClient;
import com.github.superzhc.livy.SparkLivyProxy;

/**
 * 2020年06月01日 superz add
 */
public class SparkDao
{
    private final static Logger logger = LoggerFactory.getLogger(SparkDao.class);

    private String url;
    private SparkLivyClient client;
    private Integer id;

    public SparkDao(String url) {
        this.url = url;
        client = new SparkLivyClient();
        this.id = client.getSessionId();
        logger.info("SessionId:{}", id);
    }

    public SparkDao(String url, Integer id) {
        this.url = url;
        this.id = id;
        client = new SparkLivyClient(id);
    }

    public SparkDataFrame query(String sql, String alias) {
        SparkOperateImpl sparkOperate = new SparkOperateImpl();
        SparkOperate sparkOperate1 = (SparkOperate) SparkLivyProxy.newProxyInstance(client, sparkOperate);
        // logger.debug("数据库[{}]执行语句：{}", url, sql);
        String dfKey;
        if (null == url || url == "" || url.startsWith("jdbc:hive2"))
            dfKey = sparkOperate1.hive(sql);
        else
            dfKey = sparkOperate1.jdbc(url, "(" + sql + ") " + alias);
        logger.debug("DataFrame的唯一标识：{}", dfKey);
        SparkDataFrameImpl sparkDataFrame = new SparkDataFrameImpl(dfKey, alias);
        return (SparkDataFrame) SparkLivyProxy.newProxyInstance(client, sparkDataFrame);
    }

    /**
     * 获取Livy的SessionId
     * @return
     */
    public Integer getId() {
        return id;
    }
}
