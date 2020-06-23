package com.github.superzhc.utils;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 2020年06月23日 superz add
 */
public class JavaConversionsExt
{
    public static <T> Seq<T> asScalaSeq(T... ts) {
        return asScalaSeq(Arrays.asList(ts));
    }

    public static <T> Seq<T> asScalaSeq(List<T> list) {
        if (null == list)
            return null;

        return JavaConversions.asScalaIterator(list.iterator()).toSeq();
    }
}
