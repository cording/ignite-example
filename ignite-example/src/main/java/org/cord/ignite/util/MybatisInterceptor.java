package org.cord.ignite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

/**
 * @author: cord
 * Mybatis拦截器
 * 使得ignite可以共用mybatis的配置，同时在mysql和ignite之间可动态切换
 * 通过isUseIgnite控制mybatis查询ignite还是查mysql
 * @date: 2020/7/2 20:17
 */
@Component
@Intercepts({
        @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class }),
        @Signature(type = Executor.class, method = "query",  args = { MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class })
})
public class MybatisInterceptor implements Interceptor {

    private static final Logger log = LogManager.getLogger(MybatisInterceptor.class);

    @Autowired
    private Ignite ignite;

    /**ignite通用查询默认表*/
    private static final String DEFAULT_TABLE = "student";

    /**是否从MySQL查询*/
    private static final String FROM_MYSQL = "FromMySql";

    @Value("${ignite.isDebug:false}")
    private boolean isUseIgnite;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        //如果不使用ignite，则直接返回
        if (!isUseIgnite) {
            return invocation.proceed();
        }
        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        String sqlId = mappedStatement.getId();
        String id = Arrays.stream(sqlId.split("\\.")).collect(Collectors.toCollection(LinkedList::new)).getLast();
        System.out.println(sqlId);
        System.out.println(id);
        System.out.println("-----------");
        //如果获取id失败则继续查询pg
        if(StringUtils.isEmpty(id)) {
            log.warn(String.format("ignite mybatis intercept get empty sqlId[%s], then switch to mysql.", sqlId));
            return invocation.proceed();
        }
        //如果sqlId以FROM_MYSQL结尾则继续查询pg
        if(id.endsWith(FROM_MYSQL)) {
            return invocation.proceed();
        }
        //获取sql语句
        Object parameter = null;
        if (invocation.getArgs().length > 1) {
            parameter = invocation.getArgs()[1];
        }
        String sql = getSql(mappedStatement.getConfiguration(), mappedStatement.getBoundSql(parameter));
        //获取返回类型
        Class resultType = mappedStatement.getResultMaps().get(0).getType();

        List<?> result = queryBySql(DEFAULT_TABLE, sql, resultType);
        return result;
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
    }

    /**
     * 获取执行SQL
     */
    private String getSql(Configuration configuration, BoundSql boundSql) {
        Object parameterObject = boundSql.getParameterObject();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        String sql = boundSql.getSql().replaceAll("[\\s]+", " ");
        if (parameterObject == null || parameterMappings.size() == 0) {
            return sql;
        }
        TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
        if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
            sql = sql.replaceFirst("\\?", getParameterValue(parameterObject));
        } else {
            MetaObject metaObject = configuration.newMetaObject(parameterObject);
            for (ParameterMapping parameterMapping : parameterMappings) {
                String propertyName = parameterMapping.getProperty();
                if (metaObject.hasGetter(propertyName)) {
                    Object obj = metaObject.getValue(propertyName);
                    sql = sql.replaceFirst("\\?", getParameterValue(obj));
                } else if (boundSql.hasAdditionalParameter(propertyName)) {
                    Object obj = boundSql.getAdditionalParameter(propertyName);
                    sql = sql.replaceFirst("\\?", getParameterValue(obj));
                }
            }
        }
        return sql;
    }

    private String getParameterValue(Object obj) {
        String value = null;
        if (obj instanceof String) {
            value = "'" + obj.toString() + "'";
        } else if (obj instanceof Date) {
            DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.CHINA);
            value = "'" + formatter.format(obj) + "'";
        } else {
            if (obj != null) {
                value = obj.toString();
            } else {
                value = "";
            }
        }
        return value;
    }

    /**
     * 将拦截的sql在ignite中执行并根据returnType转换成对应的类型，作为mybatis的查询结果返回
     * @param tableName
     * @param sql
     * @param pojo
     * @param <T>
     * @return
     */
    public <T> List<T> queryBySql(String tableName, String sql, Class<T> pojo) {
        IgniteCache<?, ?> cache = ignite.cache(tableName.toLowerCase()).withKeepBinary();
        FieldsQueryCursor<List<?>> qc = cache.query(new SqlFieldsQuery(sql).setDistributedJoins(true));
        List<List<?>> lists = qc.getAll();
        //如果目标类型是java内置类型，则直接转换成list
        if (pojo.isPrimitive() || pojo == String.class || pojo == BigDecimal.class) {
            List<T> result = (List<T>)lists.stream().filter(l -> !CollectionUtils.isEmpty(l)).map(l -> l.get(0)).collect(Collectors.toList());
            return result;
        }
        ObjectMapper mapper = new ObjectMapper();
        List<T> result = lists.stream()
                .map(x -> IntStream.range(0, x.size())
                        .filter(f -> !Objects.isNull(x.get(f))).boxed()
                        .collect(Collectors.toMap(i -> UPPER_UNDERSCORE.to(LOWER_CAMEL, qc.getFieldName((int) i)), i -> x.get((int) i))))
                .map(m -> mapper.convertValue(m, pojo))
                .collect(Collectors.toList());
        return result;
    }
}
