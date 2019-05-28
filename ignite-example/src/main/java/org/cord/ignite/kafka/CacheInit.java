package org.cord.ignite.kafka;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: cord
 * @date: 2019/3/21 21:08
 * 主键字段缓存初始化
 */
//@Component
public class CacheInit implements CommandLineRunner {

    @Autowired
    private Ignite ignite;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private IgniteCache<String, List<String>> pkFields;

    private static final String TABLE_PK_FIELD = "table_pk_field";

    /**
     * 初始化缓存
     */
    @Override
    public void run(String... strings) {
        pkFields = ignite.getOrCreateCache(new CacheConfiguration<String, List<String>>().setName(TABLE_PK_FIELD).setCacheMode(CacheMode.REPLICATED));
    }

    /**
     * 根据缓存名获取对应主键字段
     * @param cacheName 缓存名
     * @return
     * @throws SQLException
     */
    public List<String> getPkFields(String cacheName) throws SQLException {
        List<String> ret = pkFields.get(cacheName);
        //如果cache中有则返回,如果没有则从集群中查询元数据重新获取
        if (!CollectionUtils.isEmpty(ret)) {
            return ret;
        } else {
            return queryPkFields(cacheName);
        }
    }


    /**
     * 根据缓存名从ignite中获取源数据读取主键相关字段
     * @param cacheName 缓存名
     * @return
     * @throws SQLException
     */
    public List<String> queryPkFields(String cacheName) throws SQLException {
        try (Connection connection = jdbcTemplate.getDataSource().getConnection()) {
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet rs = meta.getPrimaryKeys(null, null, cacheName);
            List<String> list = new ArrayList<>();
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                if (columnName.equals("_KEY")) {
                    //单个主键
                    list.add(rs.getString("PK_NAME").toLowerCase());
                    return list;
                } else {
                    //联合主键
                    list.add(columnName.toLowerCase());
                }
            }
            //如果查到不为空,则将查询结果加入ignite集合并返回，否则返回null
            if (!CollectionUtils.isEmpty(list)) {
                ignite.cache(TABLE_PK_FIELD).put(cacheName, list);
                pkFields = ignite.cache(TABLE_PK_FIELD);
                return list;
            } else {
                return null;
            }
        }
    }
}
