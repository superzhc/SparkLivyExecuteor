package com.github.superzhc.livy;

import com.github.superzhc.AbstractSparkSession;
import org.apache.livy.Job;
import org.apache.livy.JobContext;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * 2020年06月01日 superz add
 */
public class SparkLivyJob<T extends AbstractSparkSession> implements Job
{
    private T target;
    private String methodName;
    private Object[] params;

    public SparkLivyJob(T target, String methodName, Object... params) {
        this.target = target;
        this.methodName = methodName;
        this.params = params;
    }

    @Override
    public Object call(JobContext jobContext) throws Exception {
        target.setSpark(jobContext.sparkSession());

        Class clazz = target.getClass();
        Method method = null;
        if (null == params || params.length == 0) {
            method = clazz.getDeclaredMethod(methodName);
        }
        else {
            Class[] paramsType = new Class[params.length];
            for (int i = 0, len = params.length; i < len; i++) {
                paramsType[i] = params[i].getClass();
            }
            method = clazz.getDeclaredMethod(methodName, paramsType);
        }
        method.setAccessible(true);
        System.out.println("调用类："+target.getClass().getSimpleName()+"执行方法：" + methodName + ";执行参数：" + Arrays.toString(params));
        return method.invoke(target, params);
    }
}
