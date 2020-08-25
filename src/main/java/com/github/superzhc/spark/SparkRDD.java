package com.github.superzhc.spark;

import com.github.superzhc.livy.SparkLivyLocal;
import com.github.superzhc.livy.SparkLivyResultProxy;
import com.github.superzhc.rdd.SparkPairRDDMapping;
import com.github.superzhc.rdd.SparkRDDMapping;
import com.github.superzhc.utils.LivySerUtils;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 2020年08月20日 superz add
 */
@SparkLivyResultProxy
public class SparkRDD<T> extends AbstractSparkSession
{
    private String rddKey;

    public SparkRDD() {
    }

    public SparkRDD(String rddKey) {
        this.rddKey = rddKey;
    }

    private JavaRDD<T> rdd() {
        return (JavaRDD<T>) SparkRDDMapping.getInstance().get(rddKey);
    }

    public int getNumPartitions(){
        return rdd().getNumPartitions();
    }

    public String toDebugString() {
        String debugString = rdd().toDebugString();
        System.out.println(debugString);
        return debugString;
    }

    /**
     * 获取元素的个数
     * @return
     */
    public long count() {
        return rdd().count();
    }

    public List<T> collect() {
        List<T> lst = rdd().collect();
        return LivySerUtils.list(lst);
    }

    public List<T> take(int num) {
        List<T> lst = rdd().take(num);
        return LivySerUtils.list(lst);
    }

    public T first() {
        return rdd().first();
    }

    public Map<T, Long> countByValue() {
        return rdd().countByValue();
    }

    // region========================操作分区=================================
    /**
     * 修改分区
     * @param num
     * @param shuffle
     *      true：会发生shuffle
     *      false：不会发生shuffle
     *      如果重分区的数量大于原理啊的分区数量，则该参数必须设置为true，否则分区数不变，因为增加分区会把原来的分区中的数据随机分配给设置的分区个数，这时一个分区会有多个分区，发生了shuffle
     */
    public SparkRDD<T> coalesce(int num, boolean shuffle) {
        JavaRDD<T> rdd2 = rdd().coalesce(num, shuffle);
        String key = SparkRDDMapping.getInstance().set(rdd2);
        return new SparkRDD<>(key);
    }

    /**
     * 修改分区，该函数是不发生shuffle的
     * 注：若增加分区数，该方法是没什么作用的
     * @param num
     * @return
     */
    public SparkRDD<T> coalesce(int num) {
        JavaRDD<T> rdd2 = rdd().coalesce(num);
        String key = SparkRDDMapping.getInstance().set(rdd2);
        return new SparkRDD<T>(key);
    }

    /**
     * 修改分区，底层实现是coalesce，该操作一定会发生shuffle
     * @param num
     * @return
     */
    public SparkRDD<T> repartition(int num) {
        JavaRDD<T> rdd2 = rdd().repartition(num);
        String key = SparkRDDMapping.getInstance().set(rdd2);
        return new SparkRDD<>(key);
    }
    // endregion=====================操作分区=================================

    // region========================持久化===================================
    public SparkRDD<T> cache() {
        rdd().cache();
        return this;
    }

    public SparkRDD<T> persist(String storageLevel) {
        rdd().persist(StorageLevel.fromString(storageLevel));
        return this;
    }

    public SparkRDD<T> unpersist() {
        rdd().unpersist();
        return this;
    }

    public SparkRDD<T> unpersist(boolean blocking) {
        rdd().unpersist(blocking);
        return this;
    }

    // 检查点
    public void checkpoint() {
        rdd().checkpoint();// 在进行检查点的时候，建议先对rdd做了缓存
    }
    // endregion=====================持久化===================================

    // region========================RDD原生算子==============================
    // 原生算子不直接对外公开，传入函数对象需要保证函数对象的类也要被上传或被依赖到Spark环境中，使用不方便
    private <T1> SparkRDD<T1> map(Function<T, T1> fun) {
        JavaRDD<T1> rdd2 = rdd().map(fun);
        String key = SparkRDDMapping.getInstance().set(rdd2);
        return new SparkRDD<>(key);
    }

    private SparkRDD<T> filter(Function<T, Boolean> fun) {
        JavaRDD<T> rdd2 = rdd().filter(fun);
        String key = SparkRDDMapping.getInstance().set(rdd2);
        return new SparkRDD<>(key);
    }
    // endregion=====================RDD原生算子==============================

    // region========================存储=====================================
    public void saveAsTextFile(String path) {
        rdd().saveAsTextFile(path);
    }

    public void saveAsObjectFile(String path) {
        rdd().saveAsObjectFile(path);
    }
    // endregion=====================存储=====================================

    @SparkLivyLocal
    public String key() {
        return rddKey;
    }
}
