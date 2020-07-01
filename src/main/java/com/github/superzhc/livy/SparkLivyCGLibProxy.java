package com.github.superzhc.livy;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.superzhc.spark.AbstractSparkSession;

import net.sf.cglib.proxy.*;

/**
 * 2020年06月19日 superz add
 */
public class SparkLivyCGLibProxy<T extends AbstractSparkSession> implements MethodInterceptor
{
    private static final Logger logger= LoggerFactory.getLogger(SparkLivyCGLibProxy.class);

    private SparkLivyClient client;
    private T target;

    public SparkLivyCGLibProxy(SparkLivyClient client, T target) {
        this.client = client;
        this.target = target;
    }

    public static <T extends AbstractSparkSession> T newProxyInstance(SparkLivyClient client, T obj) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(obj.getClass());
        enhancer.setCallback(new SparkLivyCGLibProxy(client, obj));
        return (T) enhancer.create();
    }

    @Override public Object intercept(Object object, Method method, Object[] params, MethodProxy proxy) throws Throwable {
        // 参考mybatis的对方法的拦截
        if (Object.class.equals(method.getDeclaringClass()) || //
                (null != method.getAnnotation(SparkLivyLocal.class)
                        && method.getAnnotation(SparkLivyLocal.class).value() == true)) {
            logger.debug("直接Local调用：[{}]，执行方法：{}，参数：{}", target.getClass().getSimpleName(), method.getName(),
                    Arrays.toString(params));
            return proxy.invoke(target, params);
        }

        Class clazz = target.getClass();
        logger.debug("通过Livy调用：[{}]，执行方法：[{}]，参数：{}", clazz.getSimpleName(), method.getName(), Arrays.toString(params));
        // Fixme:Livy对Arrays.ArrayList类的序列化会报空指针问题?
        /* 2020年6月9日 代理调用的方法只是将方法传递给Livy上执行同样的方法，所以可以直接将方法的参数类型直接传递给Livy上进行反射的方法 */
        Object obj = client.submit(new SparkLivyJob(target, method.getName(), method.getParameterTypes(), params));
//        if (null != obj && obj.getClass() == clazz) {// 若返回值的类型跟被代理的类型式一样的，要使返回值也被代理起来
//            return newProxyInstance(client, (T) obj);
//        }
        /* 2020年6月29日 判断返回值是否需要被代理 */
        if (null != obj) {
            SparkLivyResultProxy sparkLivyResultProxy = obj.getClass().getAnnotation(SparkLivyResultProxy.class);
            if (null != sparkLivyResultProxy && sparkLivyResultProxy.value())
                return newProxyInstance(client, (AbstractSparkSession) obj);
        }
        return obj;
    }
}
