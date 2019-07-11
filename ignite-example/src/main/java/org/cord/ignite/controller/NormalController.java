package org.cord.ignite.controller;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.builder.BinaryPlainBinaryObject;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.cord.ignite.data.domain.Student;
import org.cord.ignite.initial.CacheKeyConstant;
import org.cord.ignite.mode.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.cache.Cache;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by cord on 2018/4/11.
 * http://localhost:8080/sqlFieldsQuery
 * http://localhost:8080/sqlQuery
 * http://localhost:8080/getByKey
 * http://localhost:8080/ddlstream
 */
@RestController
public class NormalController {

    @Autowired
    private Ignite ignite;

    @Autowired
    private IgniteConfiguration igniteCfg;
    private Student student;

    @RequestMapping("/getByKey")
    public @ResponseBody
    String getByKey(HttpServletRequest request, HttpServletResponse response) {
        IgniteCache<Long, Student> cache = ignite.cache(CacheKeyConstant.STUDENT);
        Student student = cache.get(1L);
        System.out.format("The result is %s.\n", student.toString());

        return "all executed!";
    }

    /**
     * BinaryObject 流导数
     * CREATE TABLE IF NOT EXISTS PUBLIC.TEST (
     * 	STUDID INTEGER,
     * 	grade DOUBLE,
     * 	info varchar,
     * 	PRIMARY KEY (STUDID, grade))
     * WITH "template=replicated,atomicity=ATOMIC,cache_name=test,key_type=java.lang.String";
     */
    @RequestMapping("/ddlstream")
    public @ResponseBody
    String ddlByStream(HttpServletRequest request, HttpServletResponse response) {
        IgniteCache<Object, BinaryObject> cache = ignite.cache("test").withKeepBinary();
        BinaryObject student = cache.get("1");
        IgniteDataStreamer<Object, BinaryObject> dataLdr = ignite.dataStreamer("test");
        List<QueryEntity> list = (ArrayList)cache.getConfiguration(CacheConfiguration.class).getQueryEntities();
        String valueType = list.get(0).findValueType();
        BinaryObjectBuilder obj = ignite.binary().builder(valueType);
        obj.setField("studid", 22);
        obj.setField("grade", 22);
        obj.setField("info", "ss");
        dataLdr.addData("351", obj.build());
        dataLdr.close(false);
        return "all executed!";
    }
    
    @RequestMapping("/sqlFieldsQuery")
    public @ResponseBody
    String sqlFieldsQuery(HttpServletRequest request, HttpServletResponse response) {
        IgniteCache<Long, Student> cache = ignite.cache(CacheKeyConstant.STUDENT);
        SqlFieldsQuery  sfq = new SqlFieldsQuery("select name from \"student\".student");
        sfq.setReplicatedOnly(true);
        sfq.setLazy(true); //懒加载,应对海量数据
        List<List<?>> res = cache.query(sfq).getAll();
        System.out.format("The name is %s.\n", res.get(0).get(0));

        return "all executed!";
    }

    @RequestMapping("/sqlQuery")
    public @ResponseBody
    String sqlQuery(HttpServletRequest request, HttpServletResponse response) {
        IgniteCache<Long, Student> tempCache = ignite.cache(CacheKeyConstant.STUDENT);

        /**普通查询*/
        String sql_query = "name = ? and email = ?";
        SqlQuery<Long, Student> cSqlQuery = new SqlQuery<>(Student.class, sql_query);
        cSqlQuery.setReplicatedOnly(true).setArgs("student_44", "student_44gmail.com");

        List<Cache.Entry<Long, Student>> tempResult = tempCache.query(cSqlQuery).getAll();

        if (CollectionUtils.isEmpty(tempResult)) {
            return "result is Empty!";
        }
        Student student = tempResult.stream().map(t -> t.getValue()).findFirst().get();
        System.out.format("the beginning of student[student_44] is %s\n", student.getDob());

        /**域查询*/
        SqlFieldsQuery sfq = new SqlFieldsQuery("select studId, name, email from student");
//        sfq.setLazy(true); //开启懒加载防止结果集过大的情况导致堆溢出
        sfq.setReplicatedOnly(true);
        FieldsQueryCursor<List<?>> qc = tempCache.query(sfq);
        List<List<?>> result = qc.getAll();
        qc.close(); //关闭资源防止内存泄漏
        //将每一行的记录转换为map对象, key为对应的列名，value为对应的值
        List<Map<?, ?>> maps = result.stream().map(x -> IntStream.range(0, x.size()).boxed().collect(Collectors.toMap(i -> qc.getFieldName(i), i -> x.get(i)))).collect(Collectors.toList());


        /**聚合函数查询*/
        /**[count]*/
        String sql_count = "select count(1) from student";
        SqlFieldsQuery countQuery = new SqlFieldsQuery(sql_count);
        countQuery.setReplicatedOnly(true);
        List<List<?>> countList =  tempCache.query(countQuery).getAll();

        long count = 0;
        if(!CollectionUtils.isEmpty(countList)) {
            count = (Long)countList.get(0).get(0);
        }
        System.out.format("count of cache[student] is %s\n", count);

        /**[sum]*/
        String sql_sum = "select sum(studId) from student";
        SqlFieldsQuery sumQuery = new SqlFieldsQuery(sql_sum);
        sumQuery.setReplicatedOnly(true);
        List<List<?>> sumList = tempCache.query(sumQuery).getAll();
        long sum = 0;
        if(!CollectionUtils.isEmpty(sumList)) {
            sum = (Long)sumList.get(0).get(0);
        }
        System.out.format("sum of cache[student.id] is %s\n", sum);

        return "all executed!";
    }

    /**
     * 获取协调者节点
     * http://localhost:8080/coordinator
     */
    @GetMapping("/coordinator")
    public String getCoordinator() {
        TcpDiscoverySpi tds = (TcpDiscoverySpi) igniteCfg.getDiscoverySpi();
        UUID uuid = tds.getCoordinator();
        ClusterNode node = ignite.cluster().nodes().stream().filter(n -> n.id().equals(uuid)).findFirst().get();
//        String ip = node.addresses().stream().filter(a -> !"127.0.0.1".equals(a) && !"0:0:0:0:0:0:0:1".equals(a)).findFirst().get();
        return String.format("nodeId [%s], address [%s]", uuid.toString(), node.addresses().toString());
    }


    /**
     * 判断节点是否达到再平衡状态
     */
    @GetMapping("/rebalanceFinished")
    public boolean isRebalanceFinished() {
        AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(0);
        return ((IgniteKernal)ignite).context().cache().context().cacheContext(GridCacheUtils.cacheId("student")).topology().rebalanceFinished(topVer0);
    }

//    @RequestMapping("/sqlQuery")
//    public @ResponseBody
//    String sqlQuery(HttpServletRequest request, HttpServletResponse response) {
//        IgniteCache<Long, Student> cache = ignite.cache(CacheKeyConstant.STUDENT);
//
//        /**普通查询*/
//        String querySql = "salary = ?";
//        SqlQuery<Long, Person> sqlQuery = new SqlQuery<>(Person.class, querySql);
//        sqlQuery.setReplicatedOnly(true).setArgs(2);
//
//        List<Cache.Entry<Long, Person>> result = cache.query(sqlQuery).getAll();
//
//        if(CollectionUtils.isEmpty(result)){
//            return "result is Empty!";
//        }
//
//        List<Person> ret = result.stream().map(t -> t.getValue()).collect(Collectors.toList());
//
//        System.out.println(ret);
//
//        return "all executed.";
//    }
//
}
