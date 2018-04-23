package org.cord.ignite.data;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by cord on 2018/4/11.
 * 数据加载
 */
@Component
public class DataLoader {

    private static final Logger log = LogManager.getLogger(DataLoader.class);

    @Autowired
    private DataInit dataInit;

    /**导入数据到ignite*/
    public void loadData(){
        dataInit.dataInit();

    }
}
