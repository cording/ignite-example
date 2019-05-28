package org.cord.ignite.util;

import org.apache.ignite.IgniteJdbcDriver;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author: cord
 * @date: 2019/4/24 0:01
 */
@Component
public class IgniteUtil {

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
}
