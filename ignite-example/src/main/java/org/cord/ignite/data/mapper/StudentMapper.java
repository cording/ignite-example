package org.cord.ignite.data.mapper;


import org.cord.ignite.data.domain.Student;

import java.util.List;

/**
 *
 * @author cord
 * @date 2017/7/27
 */
public interface StudentMapper {

    List<Student> findAllStudents();

}
