package org.cord.ignite.initial;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;



/**
 * Created by cord on 2018/4/11.
 * ignite配置类
 */
@Configuration
public class IgniteConfig {

    private static final Logger log = LogManager.getLogger(IgniteConfig.class);

    @Autowired
    private IgniteConfiguration igniteCfg;

    @Value("ignite.isDebug")
    private String isDebug;

    @Bean
    @ConditionalOnMissingBean
    public Ignite igniteInit() {

        /**配置IGNITE_HOME*/
//        igniteCfg.setIgniteHome("D:\\Test");

        /**持久化配置*/
//        DataStorageConfiguration dcfg = igniteCfg.getDataStorageConfiguration();
//        dcfg.getDefaultDataRegionConfiguration()
//                .setMaxSize(4L * 1024 * 1024 * 1024) //设置默认区域的最大可用内存
//                .setPersistenceEnabled(true);//默认区域开启持久化
//        /**设置持久化路径*/
//        dcfg.setStoragePath();
//        dcfg.setWalPath();
//        dcfg.setWalArchivePath();

        /**是否开启debug模式*/
        if(Boolean.valueOf(isDebug)){
            igniteCfg.setFailureDetectionTimeout(Integer.MAX_VALUE);
            igniteCfg.setNetworkTimeout(Long.MAX_VALUE);
        }

        Ignite ignite = Ignition.start(igniteCfg);
        log.info("-----------ignite service is started.----------");

        return ignite;
    }
}
