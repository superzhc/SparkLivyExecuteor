package com.github.superzhc.livy;

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
            // Class[] paramsType = new Class[params.length];
            // for (int i = 0, len = params.length; i < len; i++) {
            // paramsType[i] = params[i].getClass();
            // }
            // method = clazz.getDeclaredMethod(methodName, paramsType);

            // BUG:基本类型（int,char,double,float等类型）不能被匹配的问题，如参数为9，获取的类型是java.lang.Integer，但方法中是int.class,报错
            Method[] methods = clazz.getDeclaredMethods();
            loop: for (Method m : methods) {
                if (!methodName.equals(m.getName()))
                    continue;

                Class[] parameterTypes = m.getParameterTypes();
                if (parameterTypes.length != params.length)
                    continue;

                for (int i = 0, len = parameterTypes.length; i < len; i++) {
                    if (basic2ref(parameterTypes[i]) != params[i].getClass())
                        continue loop;
                }

                method=m;
            }
        }
        method.setAccessible(true);
        System.out.println(
                "调用类：" + target.getClass().getSimpleName() + "执行方法：" + methodName + ";执行参数：" + Arrays.toString(params));
        return method.invoke(target, params);
    }

    private Class basic2ref(Class clazz) {
        if (byte.class == clazz) {
            return Byte.class;
        }
        else if (short.class == clazz) {
            return Short.class;
        }
        else if (int.class == clazz) {
            return Integer.class;
        }
        else if (long.class == clazz) {
            return Long.class;
        }
        else if (float.class == clazz) {
            return Float.class;
        }
        else if (double.class == clazz) {
            return Double.class;
        }
        else if (char.class == clazz) {
            return Character.class;
        }
        else if (boolean.class == clazz) {
            return Boolean.class;
        }
        else {
            return clazz;
        }
    }
}
