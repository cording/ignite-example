package org.cord.ignite.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cord.ignite.util.SpringUtil;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: cord
 * @date: 2019/1/19 0:10
 */
public enum DmlEnum {
    Insert {
        @Override
        public String act(String cacheName, Map<String, String> before, Map<String, String> after) {
            Set<String> keys = after.keySet();
            String[] values = after.entrySet().stream().map(e -> e.getValue()).toArray(size -> new String[size]);
            return String.format(SQL_INSERT_FORMAT, cacheName, String.join(",", keys), String.join(",", values));
        }
    },
    Update {
        private CacheInit cacheInit = SpringUtil.getBean(CacheInit.class);

        @Override
        public String act(String cacheName, Map<String, String> before, Map<String, String> after) throws SQLException {
            List<String> pkFields = cacheInit.getPkFields(cacheName);
            if (CollectionUtils.isEmpty(pkFields)) {
                log.error(String.format("cache[%s] not find primary key fields.", cacheName));
                return null;
            }
            String[] where = pkFields.stream().map(p -> String.format(SQL_ENTRY_FORMAT, p, after.get(p))).toArray(size -> new String[size]);
            String[] set = after.entrySet().stream().filter(a -> !pkFields.contains(a.getKey())).map(a -> String.format(SQL_ENTRY_FORMAT, a.getKey(), a.getValue())).toArray(size -> new String[size]);
            return String.format(SQL_UPDATE_FORMAT, cacheName, String.join(",", set), String.join(" and ", where));
        }
    },

    Delete {
        private CacheInit cacheInit = SpringUtil.getBean(CacheInit.class);

        @Override
        public String act(String cacheName, Map<String, String> before, Map<String, String> after) throws SQLException {
            List<String> pkFields = cacheInit.getPkFields(cacheName);
            if (CollectionUtils.isEmpty(pkFields)) {
                log.error(String.format("cache[%s] not find primary key fields.", cacheName));
                return null;
            }
            String[] where = pkFields.stream().map(p -> String.format(SQL_ENTRY_FORMAT, p, before.get(p))).toArray(size -> new String[size]);
            return String.format(SQL_DELETE_FORMAT, cacheName, String.join(" and ", where));
        }
    };

    private static final Logger log = LogManager.getLogger(DmlEnum.class);

    /**
     * 新增sql格式
     */
    private static final String SQL_INSERT_FORMAT = "INSERT INTO %s (%s) VALUES (%s)";

    /**
     * 更新sql格式
     */
    private static final String SQL_UPDATE_FORMAT = "UPDATE %s SET %s WHERE %s";


    /**
     * 删除sql格式
     */
    private static final String SQL_DELETE_FORMAT = "DELETE FROM %s WHERE %s";

    /**
     * sql键值对类型
     */
    private static final String SQL_ENTRY_FORMAT = "%s=%s";

    /**
     * @param valueClassName cache的value的class名
     * @param before         变化前的键值
     * @param after          变化后的键值
     * @return
     */
    public String act(String valueClassName, Map<String, String> before, Map<String, String> after) throws SQLException {
        return null;
    }

}