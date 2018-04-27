package org.cord.ignite.example.collocated;

import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Created by cord on 2018/4/27.
 */
public class Y {

    @QuerySqlField(index = true)
    private Long id;

    @QuerySqlField(index = true)
    private String info;

    @QuerySqlField(index = true)
    private Long XId;

    private transient AffinityKey<Long> key;

    public AffinityKey<Long> key(){
        if(key == null){
            key = new AffinityKey<>(id, XId);
        }
        return key;
    }

    public Y(Long id, String info, Long XId){
        this.id = id;
        this.info = info;
        this.XId = XId;
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
