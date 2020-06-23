package com.github.superzhc.spark.udf;

import com.github.superzhc.utils.JacksonUtils;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;

/**
 * 自定义UDF，继承org.apache.spark.sql.api.java.UDFxx(1-22)
 * 2020年06月23日 superz add
 */
public class CodeItem implements Serializable, UDF1<String, String>
{
    private String items;

    public CodeItem(String items) {
        this.items = items;
    }

    @Override
    public String call(String s) throws Exception {
        String value = JacksonUtils.convert(items).getString(s);
        return null == value ? "" : value;
    }
}
