package org.cord.ignite.controller;

import org.cord.ignite.mode.Organization;
import org.cord.ignite.mode.Person;
import org.cord.ignite.mode.PersonKey;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author: cord
 * @date: 2018/8/15 23:18
 */
public class AffinityMappedController {

    @Autowired
    private Ignite ignite;

    private static AtomicBoolean initflag = new AtomicBoolean(false);

    @RequestMapping("/testWithAfty")
    public @ResponseBody
    String testWithAfty(HttpServletRequest request, HttpServletResponse response) {
        if(!initflag.get()){
            return "please execute init.";
        }

        /**用sql查询缓存*/
        System.out.println("=====并置跨缓存查询=====");

        long stime = System.nanoTime();
        sqlQueryWithJoin();
        System.out.format("testWithAfty cost time [%s].\n", System.nanoTime() - stime);
        return "all executed.";
    }

    @RequestMapping("/testNoAfty")
    public @ResponseBody
    String testNoAfty(HttpServletRequest request, HttpServletResponse response) {
        if(!initflag.get()){
            return "please execute init.";
        }
        /**
         * 启用非并置的分布式关联
         * 查询映射的节点就会从远程节点通过发送广播或者单播请求的方式获取缺失的数据（本地不存在的数据）
         */
        System.out.println("=====非并置跨缓存查询=====");

        long stime =System.nanoTime();
        sqlQueryWithDistributedJoin();
        System.out.format("testNoAfty cost time [%s].\n", System.nanoTime() - stime);
        return "all executed.";
    }

    @RequestMapping("/init")
    public @ResponseBody
    String affinityInit(HttpServletRequest request, HttpServletResponse response) {

        if(initflag.get()){
            return "already init.";
        }

        /**并置针对的是分区模式的数据*/
        CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>("Organizations");
        orgCacheCfg.setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Long.class, Organization.class);

        CacheConfiguration<PersonKey, Person> colPersonCacheCfg = new CacheConfiguration<>("CollocatedPersons");
        colPersonCacheCfg.setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(PersonKey.class, Person.class);

        CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<>("Persons");
        personCacheCfg.setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Long.class, Person.class);

        /**创建缓存*/
        ignite.destroyCache("Organizations");
        ignite.destroyCache("CollocatedPersons");
        ignite.destroyCache("Persons");
        ignite.getOrCreateCache(orgCacheCfg);
        ignite.getOrCreateCache(colPersonCacheCfg);
        ignite.getOrCreateCache(personCacheCfg);

        /*********************************************************************************************/

        IgniteCache<Long, Organization> orgCache = ignite.cache("Organizations");

        // Clear cache before running the example.
        orgCache.clear();

        // Organizations.
        Organization org1 = new Organization("ApacheIgnite");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id(), org1);
        orgCache.put(org2.id(), org2);

        IgniteCache<PersonKey, Person> colPersonCache = ignite.cache("CollocatedPersons");
        IgniteCache<Long, Person> personCache = ignite.cache("Persons");

        // Clear caches before running the example.
        colPersonCache.clear();
        personCache.clear();

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        colPersonCache.put(new PersonKey(p1.id, org1.id()), p1);
        colPersonCache.put(new PersonKey(p2.id, org1.id()), p2);
        colPersonCache.put(new PersonKey(p3.id, org2.id()), p3);
        colPersonCache.put(new PersonKey(p4.id, org2.id()), p4);

        // These Person objects are not collocated with their organizations.
        personCache.put(p1.id, p1);
        personCache.put(p2.id, p2);
        personCache.put(p3.id, p3);
        personCache.put(p4.id, p4);

        initflag.set(true);

        return "all executed.";
    }

    /**并置查询*/
    private void sqlQueryWithJoin() {
        IgniteCache<PersonKey, Person> cache = ignite.cache("CollocatedPersons");
//        ignite.cluster().forRemotes().ignite().cache("CollocatedPersons").lock(1);

        /**
         *关联的缓存需要指定模式名(缓存名)
         * 例如下面这个sql关联Organization的时候需要加上Organizations作为前缀
         * 而Person的缓存Persons会作为默认模式名，所以不需要额外指定
         *
         */
        String joinSql = "from Person, \"Organizations\".Organization as org " +
                "where Person.orgId = org.id " +
                "and lower(org.name) = lower(?)";

        print("Following people are 'ApacheIgnite' employees: ",
                cache.query(new SqlQuery<PersonKey, Person>(Person.class, joinSql).
                        setArgs("ApacheIgnite")).getAll());

        print("Following people are 'Other' employees: ",
                cache.query(new SqlQuery<PersonKey, Person>(Person.class, joinSql).
                        setArgs("Other")).getAll());
    }

    /**非并置查询*/
    private void sqlQueryWithDistributedJoin() {
        IgniteCache<Long, Person> cache = ignite.cache("Persons");
        String joinSql =
                "from Person, \"Organizations\".Organization as org " +
                        "where Person.orgId = org.id " +
                        "and lower(org.name) = lower(?)";
        SqlQuery<Long, Person> qry = new SqlQuery<>(Person.class, joinSql);

        /**
         * 启用非并置的分布式关联
         * 查询映射的节点就会从远程节点通过发送广播或者单播请求的方式获取缺失的数据（本地不存在的数据）
         */
        qry.setDistributedJoins(true);

        print("Following people are 'ApacheIgnite' employees (distributed join): ", cache.query(qry.setArgs("ApacheIgnite")).getAll());
        print("Following people are 'Other' employees (distributed join): ", cache.query(qry.setArgs("Other")).getAll());
    }

    private static void print(String msg, Iterable<?> col) {
        System.out.println(">>> " + msg);
        System.out.println(">>> " + col);

    }

}