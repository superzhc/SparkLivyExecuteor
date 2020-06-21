package com.github.superzhc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.superzhc.livy.SparkLivy;
import com.github.superzhc.spark.SparkDataFrame;
import com.github.superzhc.spark.SparkSQL;

import java.util.List;

/**
 * 2020年06月20日 superz add
 */
public class SparkLivyDao
{
    private static final Logger logger = LoggerFactory.getLogger(SparkLivyDao.class);

    private SparkLivy sparkLivy;
    private volatile SparkSQL sparkSQL;

    public SparkLivyDao() {
        this.sparkLivy = new SparkLivy();
    }

    public SparkLivyDao(Integer sessionId) {
        this.sparkLivy = new SparkLivy(sessionId);
    }

    public Integer sessionId(){
        return sparkLivy.getSessionId();
    }

    private SparkSQL dao() {
        if (null == sparkSQL) {
            synchronized (this) {
                if (null == sparkSQL) {
                    sparkSQL = sparkLivy.cglib(new SparkSQL());
                }
            }
        }
        return sparkSQL;
    }

    private SparkDataFrame dataFrame(String dfKey, String tableName) {
        SparkDataFrame sparkDataFrame = new SparkDataFrame(dfKey, tableName);
        return sparkLivy.cglib(sparkDataFrame);
    }

    public SparkDataFrame jdbc(String url, String sql, String tableName) {
        String dfKey = dao().jdbc(url, "(" + sql + ") " + tableName);
        return dataFrame(dfKey, tableName);
    }

    public SparkDataFrame jdbc4Partition(String url, String sql, String tableName, String partitionColumn,
            Long lowerBound, Long upperBound, Integer numPartitions) {
        String dfKey = dao().jdbc4Partition(url, "(" + sql + ") " + tableName, partitionColumn, lowerBound, upperBound,
                numPartitions);
        return dataFrame(dfKey, tableName);
    }

    public SparkDataFrame hive(String sql, String tableName) {
        String dfKey = dao().hive(sql);
        return dataFrame(dfKey, tableName);
    }

    public SparkDataFrame json(List<String> jsons, String tableName) {
        String dfKey = dao().json(jsons);
        return dataFrame(dfKey, tableName);
    }
}
