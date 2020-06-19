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

    public static SparkDataFrame join(SparkDataFrame df, SparkDataFrame otherDf, String joinType, String... columns) {
        return df.join(otherDf.key(), columns, joinType);
    }

    public static SparkDataFrame join(SparkDataFrame df, SparkDataFrame otherDf, String alias, String joinType,
            String... columns) {
        return df.join(otherDf.key(), columns, joinType, alias);
    }
}
