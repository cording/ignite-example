package org.cord.ignite.controller;

import com.google.gson.Gson;
import org.cord.ignite.data.domain.Student;
import org.cord.ignite.data.mapper.IgniteMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
    private Gson gson;

    @GetMapping("/findStudentsById")
    public String findStudentsById(String studentId) {
        Student student = igniteMapper.findStudentsById(studentId);
        String result = gson.toJson(student);
        System.out.println(result);
        return result;
    }

}
