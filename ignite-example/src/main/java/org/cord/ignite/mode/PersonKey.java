package org.cord.ignite.mode;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * @author: cord
 * @date: 2018/8/15 23:05
 */
public class PersonKey {

    private long id;

    @AffinityKeyMapped
    private long organizationId;

    public PersonKey(){
    }

    public PersonKey(long id, long organizationId){
        this.id = id;
        this.organizationId = organizationId;
    }

    public long id(){
        return id;
    }

    public long organizationId(){
        return organizationId;
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }

        if(o == null || getClass() != o.getClass()){
            return false;
        }

        PersonKey key = (PersonKey)o;
        return id == key.id && organizationId == key.organizationId;
    }

}
