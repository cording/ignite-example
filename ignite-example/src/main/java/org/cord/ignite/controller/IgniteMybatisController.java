package org.cord.ignite.controller;

import com.google.gson.Gson;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.cord.ignite.data.domain.Student;
import org.cord.ignite.data.mapper.IgniteMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author: cord
 * @date: 2019/1/16 23:01
 */
@RestController
@RequestMapping("/mybatis")
public class IgniteMybatisController {

    @Autowired
    private IgniteMapper igniteMapper;

    @Autowired
    private Ignite ignite;

    @Autowired
    private Gson gson;

    @GetMapping("/findStudentsById")
    public String findStudentsById(String studentId) {
        long start = System.currentTimeMillis();
        Student student = igniteMapper.findStudentsById(studentId);
        System.out.printf("/findStudentsById 耗时 [%s]ms.\n", System.currentTimeMillis() - start);
        String result = gson.toJson(student);
//        System.out.println(result);
        return result;
    }

    @GetMapping("/findGradeByName")
    public double findGradeByName(String name) {
       double grade = igniteMapper.findGradeByName(name);
       return grade;
    }

    /**
     * api查询性能对比测试
     */
    @GetMapping("/cpFindStudentsById")
    public String cpFindStudentsById(String studentId) {
        IgniteCache<String, Student> cache = ignite.cache("student");
        SqlFieldsQuery sfq = new SqlFieldsQuery("select * from student where studid=?").setArgs(6).setReplicatedOnly(true);
        long start = System.currentTimeMillis();
        FieldsQueryCursor<List<?>> qc = cache.query(sfq);
        System.out.printf("/cpFindStudentsById 耗时 [%s]ms.\n", System.currentTimeMillis() - start);
        List<Map<?, ?>> maps = qc.getAll().stream().map(x -> IntStream.range(0, x.size()).boxed().collect(Collectors.toMap(i -> qc.getFieldName(i), i -> x.get(i)))).collect(Collectors.toList());
        String str = gson.toJson(maps);
//        System.out.println(str);
        return str;
    }

}
