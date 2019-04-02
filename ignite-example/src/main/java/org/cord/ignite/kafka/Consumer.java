package org.cord.ignite.kafka;

import com.google.gson.Gson;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.cache.CacheException;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author: cord
 * @date: 2019/3/22 19:05
 */
@Component
public class Consumer {

    private static final Logger log = LogManager.getLogger(Consumer.class);

    @Autowired
    private Gson gson;

    @Autowired
    private Ignite ignite;

    @KafkaListener(topics = "#{'${kafka.topics}'.split(',')}", containerFactory = "kafkaListenerContainerFactory")
    public void receive(ConsumerRecord<?, ?> consumer, Acknowledgment ack) throws SQLException {
        GoldenGateMsg ggm = gson.fromJson(consumer.value().toString(), GoldenGateMsg.class);
        //提取变化前的数据
        Map<String, String> before = ggm.getBefore() == null ? null : gson.fromJson(ggm.getBefore().toString(), Map.class);
        //提取变化后的数据
        Map<String, String> after = ggm.getAfter() == null ? null : gson.fromJson(ggm.getAfter().toString(), Map.class);
        //获取缓存名
        String cacheName = getCacheName(ggm.getTable());
        //生成对应的sql
        String type = KafkaConstants.DML_TYPE.get(ggm.getOpType());
        DmlEnum de = DmlEnum.valueOf(type);
        String sql = de.act(cacheName, before, after);
        //执行sql
        executeIgniteSql(cacheName, sql);
        log.info(String.format("相关sql为: %s", sql));
        //commit offset
        ack.acknowledge();
    }

    /**
     * 提取cache名
     *
     * @param tableName 表名
     * @return cache名
     */
    public static String getCacheName(String tableName) {
        String[] arrays = tableName.split("\\.");
        return arrays.length == 2 ? arrays[1] : tableName;
    }

    /**
     * 在缓存上执行sql
     *
     * @param cacheName 缓存名
     * @param sql       执行sql
     */
    public void executeIgniteSql(String cacheName, String sql) {
        try {
            ignite.cache(cacheName).query(new SqlFieldsQuery(sql)).getAll();
        } catch (CacheException ce) {
            //如果是主键冲突，则捕获并打印异常，不然则继续抛出
            if (!StringUtils.isEmpty(ce.getMessage()) && ce.getMessage().indexOf("Duplicate key during INSERT") != -1) {
                log.error(String.format("逐渐冲突：table[%s], sql[%s], exception[%s]", cacheName, sql, ce.getMessage()));
            } else {
                throw ce;
            }
        }
    }
}