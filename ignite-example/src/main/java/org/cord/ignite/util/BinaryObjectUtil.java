package org.cord.ignite.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cord.ignite.kafka.CacheInit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author: cord
 * @date: 2019/7/2 22:19
 * BinaryObject相关操作
 */
@Component
public class BinaryObjectUtil {

    private static final Logger log = LogManager.getLogger(BinaryObjectUtil.class);

    @Autowired
    private Ignite ignite;

    @Autowired
    private CacheInit cacheInit;

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
     *
     * @param keyType   key类型值
     * @param valueType value类型值
     * @param list      要存入的数据集合，其中每一个map对应数据库中的一行数据
     * @param ids       流对象
     * @param pkFields  表的主键字段信息
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

    private static final int pageSize = 10000;

    /**
     * 根据表名使用流往ignite中导数据
     */
    public void loadTable(String tableName) {
        try (IgniteDataStreamer<Object, BinaryObject> ids = ignite.dataStreamer(tableName)) {
            log.info(String.format("-----开始导入[%s]-----", tableName));
            String keyType = getKeyType(tableName);
            String valueType = getValueType(tableName);
            ids.allowOverwrite(true);
            List<String> pkFileds = cacheInit.getPkFields(tableName);
            if (CollectionUtils.isEmpty(pkFileds)) {
                throw new RuntimeException(String.format("cache[%s] not find primary key fields.", tableName));
            }
            ExecutorService pool = DATA_POOL;
            List<Future<String>> results = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            List<String> dbList = new ArrayList<>();//防止检查报错，真实情况中则是数据源名
            dbList.forEach(d -> {
//                DynamicDataSourceContextHolder.setDateSoureType(d);
                Map<String, Object> params = new HashMap<>(6);
                params.put("tableName", tableName);
                long total = /**dao.countTable(params);*/100;
                int totalPage = (int) (total % pageSize == 0 ? total / pageSize : total / pageSize + 1);
                params.put("pageCount", totalPage);


                for (int i = 1; i <= totalPage; i++) {
                    final int j = i;
                    Future<String> future = pool.submit(() -> {
                        Map<String, Object> param = new HashMap<>(6);
                        param.putAll(params);
                        param.put("pageCount", totalPage);
                        param.put("currentPage", j);
//                        DynamicDataSourceContextHolder.setDateSoureType(d);
                        List<Map<String, Object>> list = /**dao.queryTable(param);*/ new ArrayList<>();
                        streamingBinaryObject(keyType, valueType, list, ids, pkFileds);
                        return String.format(String.format("数据源[%s]，[%s]，导入第[%s]页数据，数据量大小为[%s].", d, tableName, j), list == null ? 0 : list.size());
                    });

                    results.add(future);
                }
            });
            for (Future<String> future : results) {
                try {
                    log.info(future.get(5, TimeUnit.MINUTES));
                } catch (TimeoutException te) {
                    future.cancel(true);
                    log.error(te.getMessage(), te);
                }
            }
            ids.tryFlush();
            log.info(String.format("导入表 [%s] 消耗时间 [%s]ms.\n", tableName, System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 导数线程池
     */
    public static ExecutorService DATA_POOL = new ThreadPoolExecutor(8, 16, 2L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(8), new ThreadFactoryBuilder().setNameFormat("data-pool-%d").build(),
            (Runnable r, ThreadPoolExecutor executor) -> {
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            });

}
