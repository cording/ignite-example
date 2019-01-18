package org.cord.ignite.data.domain;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by cord on 2017/7/27.
 */
public class Grade implements Serializable{
    @QuerySqlField
    private Integer studId;

    @QuerySqlField
    private Double grade;

    public Integer getStudId() {
        return studId;
    }

    public void setStudId(Integer studId) {
        this.studId = studId;
    }

    public Double getGrade() {
        return grade;
    }

    public void setGrade(Double grade) {
        this.grade = grade;
    }
}
