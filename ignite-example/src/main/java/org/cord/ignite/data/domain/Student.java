package org.cord.ignite.data.domain;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by cord on 2017/7/27.
 */
public class Student implements Serializable{
    @QuerySqlField
    private Integer studId;

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(
            name = "student_ne_index", order = 0)})
    private String name;
    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(
            name = "student_ne_index", order = 1)})
    private String email;
    @QuerySqlField
    private Date dob;

    public Integer getStudId() {
        return studId;
    }

    public void setStudId(Integer studId) {
        this.studId = studId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Date getDob() {
        return dob;
    }

    public void setDob(Date dob) {
        this.dob = dob;
    }
}
