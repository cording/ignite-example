package org.cord.ignite.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: cord
 * @date: 2019/7/2 22:19
 * BinaryObject相关操作
 */
@Component
public class BinaryObjectUtil {

    @Autowired
    private Ignite ignite;

    /**
     * 获取ddl定义的表的keyType值
     */
    public String getKeyType(String cacheName) {
        IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName.toLowerCase()).withKeepBinary();
        List<QueryEntity> list = (ArrayList) cache.getConfiguration(CacheConfiguration.class).getQueryEntities();
        if (list.size() == 1) {
            return list.get(0).findKeyType();
        } else {
            throw new RuntimeException(String.format("cache[%s] not find key type\n", cacheName));
        }
    }

    /**
     * 获取ddl定义的表的valueType值
     */
    public String getValueType(String cacheName) {
        IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName.toLowerCase()).withKeepBinary();
        List<QueryEntity> list = (ArrayList) cache.getConfiguration(CacheConfiguration.class).getQueryEntities();
        if (list.size() == 1) {
            return list.get(0).findValueType();
        } else {
            throw new RuntimeException(String.format("cache[%s] not find value type\n", cacheName));
        }
    }

    /**
     * 构建BinaryObject并使用流导入
     * @param keyType  key类型值
     * @param valueType value类型值
     * @param list  要存入的数据集合，其中每一个map对应数据库中的一行数据
     * @param ids   流对象
     * @param pkFields 表的主键字段信息
     */
    public void streamingBinaryObject(String keyType, String valueType, List<Map<String, Object>> list, IgniteDataStreamer<Object, BinaryObject> ids, List<String> pkFields) {
        IgniteBinary ib = ignite.binary();
        for (Map<String, Object> m : list) {
            BinaryObjectBuilder key = ib.builder(keyType);
            BinaryObjectBuilder value = ib.builder(valueType);
            m.entrySet().forEach(e -> value.setField(e.getKey(), validateValue(e.getValue())));
            //单一主键
            if (pkFields.size() == 1) {
                ids.addData(validateKey(pkFields.get(0), m), value.build());
            } else {
                //联合主键
                pkFields.forEach(p -> key.setField(p, validateKey(p, m)));
                ids.addData(key.build(), value.build());
            }
        }
    }

    /**
     * 校验key类型，使得源数据类型与ignite里的数据类型一致
     */
    private static Object validateKey(String key, Map<String, Object> map) {
        Object obj = map.get(key);
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        }
        return obj;
    }

    /**
     * 校验value类型，使得源数据类型与ignite里的数据类型一致
     */
    private static Object validateValue(Object obj) {
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        }
        return obj;
    }

}
