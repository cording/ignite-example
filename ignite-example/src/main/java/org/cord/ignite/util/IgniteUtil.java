package org.cord.ignite.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author: cord
 * @date: 2019/4/24 0:01
 */
@Component
public class IgniteUtil {

    @Autowired
    private Ignite ignite;

    /**
     * jdbc批量插入
     * @param sqls
     * @throws SQLException
     */
    public void igniteJdbcBatchInsert(String[] sqls) throws SQLException {
        String url = "jdbc:ignite:thin://127.0.0.1/";
//        Properties properties = new Properties();
//        properties.setProperty(IgniteJdbcDriver.PROP_STREAMING, "true");
//        properties.setProperty(IgniteJdbcDriver.PROP_STREAMING_ALLOW_OVERWRITE, "true");
//        try (Connection conn = DriverManager.getConnection(url, properties)){
        try (Connection conn = DriverManager.getConnection(url)){
            Statement statement = conn.createStatement();
            statement.execute("set streaming on ordered");
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
            statement.execute("set streaming off");
        }
    }

    private static final int pageSize = 500;

    /**
     * 将ignite的数据迭代写进关系型数据库
     * 相关mapper如下
     *
     * <insert id="batchInsert" parameterType="java.util.Map">
     *         insert into ${tableName}
     *         <foreach collection="fields" item="field" open="(" close=")" separator=",">
     *             ${field}
     *         </foreach>
     *         values
     *         <foreach collection="nestList" item="item1" separator=",">
     *             <foreach collection="item1" item="item2" open="(" close=")" separator=",">
     *                 #{item2}
     *             </foreach>
     *         </foreach>
     *     </insert>
     * @param tableName
     */
    public void loadFromIgniteToDb(String tableName) {

        IgniteCache<String, BinaryObject> cache = ignite.cache(tableName.toLowerCase()).withKeepBinary();
        if(cache.size() == 0) {
            return;
        }
        String sql = String.format("SELECT * FROM %s", tableName);
        try (FieldsQueryCursor<List<?>> qc = cache.query(new SqlFieldsQuery(sql).setLazy(true))) {
            int columns = qc.getColumnsCount();
            List<String> fields = IntStream.range(0, columns).mapToObj(c -> qc.getFieldName(c)).collect(Collectors.toList());
            Map<String, Object> maps = new HashMap<>(2);
            maps.put("tableName", tableName.toLowerCase());
            maps.put("fields", fields);

            Iterator<List<?>> iterator = qc.iterator();
            int count = 0;
            List<List<?>> data = new ArrayList<>();
            while (iterator.hasNext()) {
                data.add(iterator.next());
                count++;
                if (count == pageSize) {
                    List<List<?>> copy = data;
                    Map<String, Object> params = new HashMap<>(maps);
                    if(CollectionUtils.isEmpty(copy)) {
                        return;
                    }
                    params.put("nestList", copy);
//                    daoService.batchInsertData(params);
                    count = 0;
                    data = new ArrayList<>();
                }
            }
            Map<String, Object> params = new HashMap<>(maps);
            if(CollectionUtils.isEmpty(data)) {
                return;
            }
            params.put("nestList", data);
//            daoService.batchInsertData(params);
//            Log.info(String.format("------syn data from ignite to db success, tableName[%s].\n", tableName));
        } catch (Exception e) {
//            Log.error(String.format("------syn data from ignite to db error, tableName[%s].\n", tableName), e);
        }
    }
}
