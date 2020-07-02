package com.github.superzhc.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Livy 序列化工具
 * 2020年07月02日 superz add
 */
public class LivySerUtils
{
    /**
     * Spark算子直接返回的列表，Livy无法进行序列化
     * @param lst
     * @param <T>
     * @return
     */
    public static <T> List<T> list(List<T> lst) {
        List<T> result = new ArrayList<>(lst.size());
        for (T e : lst) {
            result.add(e);
        }
        return result;
    }

    /**
     * 使用Arrays.asList转换成的列表，在Livy序列化调用java.util.Arrays$ArrayList.size报空指针
     * @param array
     * @param <T>
     * @return
     */
    public static <T> List<T> array2list(T[] array) {
        List<T> lst = new ArrayList<>(array.length);
        for (T row : array) {
            lst.add(row);
        }
        return lst;
    }
}
