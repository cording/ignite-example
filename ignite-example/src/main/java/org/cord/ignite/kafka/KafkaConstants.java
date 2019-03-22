package org.cord.ignite.kafka;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: cord
 * @date: 2019/3/22 19:09
 */
public class KafkaConstants {
    /**
     * goldengate 操作类型
     */
    public static final Map<String, String> DML_TYPE = new HashMap<String, String>() {{
        put("I", "Insert");
        put("U", "Update");
        put("D", "Delete");
    }};
}
