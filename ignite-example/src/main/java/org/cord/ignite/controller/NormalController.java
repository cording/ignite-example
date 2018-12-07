package org.cord.ignite.controller;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.cord.ignite.data.domain.Student;
import org.cord.ignite.initial.CacheKeyConstant;
import org.cord.ignite.mode.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.cache.Cache;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by cord on 2018/4/11.
 * http://localhost:8080/sqlFieldsQuery
 * http://localhost:8080/sqlQuery
 * http://localhost:8080/getByKey
 */
@Controller
public class NormalController {

    @Autowired
    private Ignite ignite;

    @RequestMapping("/getByKey")
    public @ResponseBody
    String getByKey(HttpServletRequest request, HttpServletResponse response) {
        IgniteCache<Long, Student> cache = ignite.cache(CacheKeyConstant.STUDENT);
        Student student = cache.get(1L);
        System.out.format("The result is %s.\n", student.toString());

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
