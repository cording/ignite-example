package org.cord.ignite.controller;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.cord.ignite.initial.CacheKeyConstant;
import org.cord.ignite.mode.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.cache.Cache;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by cord on 2018/4/11.
 */
@Controller
public class NormalController {

    @Autowired
    private Ignite ignite;

    /**使用sql查询缓存*/
    @RequestMapping("/sqlQuery")
    public @ResponseBody
    String sqlQuery(HttpServletRequest request, HttpServletResponse response) {
        IgniteCache<Long, Person> cache = ignite.cache(CacheKeyConstant.PERSON);

        /**普通查询*/
        String querySql = "salary = ?";
        SqlQuery<Long, Person> sqlQuery = new SqlQuery<>(Person.class, querySql);
        sqlQuery.setReplicatedOnly(true).setArgs(2);

        List<Cache.Entry<Long, Person>> result = cache.query(sqlQuery).getAll();

        if(CollectionUtils.isEmpty(result)){
            return "result is Empty!";
        }

        List<Person> ret = result.stream().map(t -> t.getValue()).collect(Collectors.toList());

        System.out.println(ret);

        return "all executed.";
    }
}
