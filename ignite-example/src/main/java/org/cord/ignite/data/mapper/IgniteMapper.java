package org.cord.ignite.data.mapper;

import org.cord.ignite.data.domain.Student;

/**
 * @author: cord
 * @date: 2019/1/16 22:54
 */
public interface IgniteMapper {

    /**
     * 根据studentId查询学生信息
     * @param studentId
     * @return Student
     */
    Student findStudentsById(String studentId);

}
