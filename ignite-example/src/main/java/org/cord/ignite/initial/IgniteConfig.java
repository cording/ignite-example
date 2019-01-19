package org.cord.ignite.initial;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DatabasePopulator;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.io.IOException;
import java.util.Map;


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

    /**通过代码初始化datasource*/
//    @Bean(name = "igniteDS")
    public DriverManagerDataSource dataSource() throws IOException {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.apache.ignite.IgniteJdbcThinDriver");
        dataSource.setUrl("jdbc:ignite:thin://127.0.0.1");
//        dataSource.setUsername("");
//        dataSource.setPassword("");

        // schema init
        Resource[] initSchema = new PathMatchingResourcePatternResolver().getResources("classpath:db/schema.sql");
//        Resource initData = new ClassPathResource("db/data.sql");
        DatabasePopulator databasePopulator = new ResourceDatabasePopulator(initSchema);
        DatabasePopulatorUtils.execute(databasePopulator, dataSource);
        return dataSource;
    }

    @Bean
    public Gson gson() {
        return new GsonBuilder()
                .serializeNulls()
                .registerTypeAdapter(Map.class, new MapDeserializer())
//                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .setDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
                .create();
    }
}
