package org.cord.ignite.kafka;

import org.h2.command.dml.Insert;

import java.util.Map;
import java.util.Set;

/**
 * @author: cord
 * @date: 2019/1/19 0:10
 */
public enum DmlEnum {

    Insert{
        @Override
        public String act(String valueClassName, Map<String, String> before, Map<String, String> after) {
            Set<String> keys = after.keySet();
            String[] values = after.entrySet().stream().map(e -> e.getValue()).toArray(size -> new String[size]);
            return String.format(SQL_INSERT_FORMAT, valueClassName, String.join(",", keys), String.join(",", values));
        }
    },
    Update{
        @Override
        public String act(String valueClassName, Map<String, String> before, Map<String, String> after) {
            String[] bf = before.entrySet().stream().map(b -> String.format(SQL_ENTRY_FORMAT, b.getKey(), b.getValue())).toArray(size -> new String[size]);
            String[] af = before.entrySet().stream().map(a -> String.format(SQL_ENTRY_FORMAT, a.getKey(), a.getValue())).toArray(size -> new String[size]);
            return String.format(SQL_UPDATE_FORMAT, valueClassName, String.join(",", af), String.join(",", bf));
        }
    };

    /**新增sql格式*/
    private static final String SQL_INSERT_FORMAT = "INSERT INTO %S (%S) VALUES (%s)";

    /**更新sql格式*/
    private static final String SQL_UPDATE_FORMAT = "UPDATE %s SET %s WHERE %s";

    /**sql键值对类型*/
    private static final String SQL_ENTRY_FORMAT = "%s=%s";

    /**
     * @param valueClassName cache的value的class名
     * @param before 变化前的键值
     * @param after 变化后的键值
     * @return
     */
    public String act(String valueClassName, Map<String, String> before, Map<String, String> after) { return null; } }
