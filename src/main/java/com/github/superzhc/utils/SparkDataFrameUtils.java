package com.github.superzhc.utils;

import com.github.superzhc.dataframe.SparkDataFrame;

/**
 * 2020年06月19日 superz add
 */
public class SparkDataFrameUtils
{
    public static SparkDataFrame union(SparkDataFrame df, SparkDataFrame otherDf) {
        return df.union(otherDf.key());
    }

    public static SparkDataFrame union(SparkDataFrame df, SparkDataFrame otherDf, String alias) {
        return df.union(otherDf.key(), alias);
    }

    public static SparkDataFrame unionAll(SparkDataFrame df, SparkDataFrame otherDf) {
        return df.unionAll(otherDf.key());
    }

    public static SparkDataFrame unionAll(SparkDataFrame df, SparkDataFrame otherDf, String alias) {
        return df.unionAll(otherDf.key(), alias);
    }
}
