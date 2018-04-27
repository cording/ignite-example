package org.cord.ignite.example.collocated;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Created by cord on 2018/4/27.
 */
public class X {

    @QuerySqlField(index = true)
    private Long id;

    private String info;

    public X(Long id, String info){
        this.id = id;
        this.info = info;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
}
