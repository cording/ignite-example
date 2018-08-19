package org.cord.ignite.controller;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.cord.ignite.mode.Organization;
import org.cord.ignite.mode.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.cache.Cache;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * @author: cord
 * @date: 2018/8/15 23:09
 */
@Controller
@RequestMapping("/affinityKey")
public class AffinityKeyController {

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

        sqlQueryWithJoin();
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

        sqlQueryWithDistributedJoin();
        return "all executed.";
    }

    @RequestMapping("/testWithExplain")
    public @ResponseBody
    String testWithExplain(HttpServletRequest request, HttpServletResponse response) {
        if(!initflag.get()){
            return "please execute init.";
        }
        /**用sql查询缓存*/
        System.out.println("=====并置跨缓存查询explain=====");

        IgniteCache<AffinityKey<Long>, Person> cache = ignite.cache("CollocatedPersons");
        String joinSql = "select * from Person, \"Organizations\".Organization as org " +
                "where Person.orgId = org.id " +
                "and Person.salary = ?" +
                "and lower(org.name) = lower(?)";
        int i = IntStream.range(1, 101).skip((int)(100*Math.random())).findFirst().getAsInt();

        SqlFieldsQuery qry = new SqlFieldsQuery("EXPLAIN " + joinSql);
        qry.setArgs(1000*i, "ApacheIgnite");
        String plan = (String)cache.query(qry).getAll().get(0).get(0);
        System.out.format("sql解析结果: [%s].\n", plan);

        return "all executed.";
    }

    @RequestMapping("/testAftyCompute")
    public @ResponseBody
    String testAftyCompute(HttpServletRequest request, HttpServletResponse response){
        if(!initflag.get()){
            return "please execute init.";
        }

        IgniteCompute compute = ignite.compute();
//        IgniteCache<AffinityKey<Long>, Person> cache = ignite.cache("CollocatedPersons");
        IgniteCache<AffinityKey<Long>, BinaryObject> cache = ignite.cache("CollocatedPersons").withKeepBinary();

        String joinSql = "from Person, \"Organizations\".Organization as org " +
                "where Person.orgId = org.id " +
                "and Person.salary = ?" +
                "and lower(org.name) = lower(?)";

//        ExecutorService exec = ignite.executorService();
        ExecutorService exec = Executors.newFixedThreadPool(110);
        final CountDownLatch startGate = new CountDownLatch(1);
//        final CountDownLatch endGate = new CountDownLatch(100);

        for(int j=0; j<100; j++){
            int i = IntStream.range(1, 101).skip((int)(100*Math.random())).findFirst().getAsInt();
            exec.execute(() -> {
                try {
                    startGate.await();
                    compute.affinityRun("CollocatedPersons", i, new IgniteRunnable() {
                        @Override
                        public void run() {
//                            long stime = System.nanoTime();
//                            List<Cache.Entry<AffinityKey<Long>, Person>> result = cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, joinSql).
//                                    setArgs(1000*i, "ApacheIgnite")).getAll();
//                            System.out.format("testAftyCompute cost time [%s].\n", (System.nanoTime() - stime)/1000000.00);
//                            print("Following people are 'ApacheIgnite' employees: ", result);


                            long stime = System.nanoTime();
                            List<Cache.Entry<AffinityKey<Long>, BinaryObject>> result = cache.query(new SqlQuery<AffinityKey<Long>, BinaryObject>(Person.class, joinSql).
                                    setArgs(1000 * i, "ApacheIgnite")).getAll();
                            System.out.format("testAftyCompute cost time [%s].\n", (System.nanoTime() - stime) / 1000000.00);
                            print("Following people are 'ApacheIgnite' employees: ", result);
                        }
                    });
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
        startGate.countDown();

//        compute.affinityRun("CollocatedPersons", i, () -> {
//            long stime = System.nanoTime();
//            List<Cache.Entry<AffinityKey<Long>, Person>> result = cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, joinSql).
//                    setArgs(1000*i, "ApacheIgnite")).getAll();
//            System.out.format("testAftyCompute cost time [%s].\n", (System.nanoTime() - stime)/1000000.00);
//            print("Following people are 'ApacheIgnite' employees: ", result);
//        });

        /**数据并置加计算并置*/

        return "all executed.";
    }

    @RequestMapping("/init")
    public @ResponseBody
    String affinityInit(HttpServletRequest request, HttpServletResponse response) {

//        if(initflag.get()){
//            return "already init.";
//        }

        /**并置针对的是分区模式的数据*/
        CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>("Organizations");
        orgCacheCfg.setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Long.class, Organization.class);

        CacheConfiguration<AffinityKey<Long>, Person> colPersonCacheCfg = new CacheConfiguration<>("CollocatedPersons");
        colPersonCacheCfg.setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(AffinityKey.class, Person.class);

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

        IgniteCache<AffinityKey<Long>, Person> colPersonCache = ignite.cache("CollocatedPersons");
        IgniteCache<Long, Person> personCache = ignite.cache("Persons");

        // Clear caches before running the example.
        colPersonCache.clear();
        personCache.clear();

//        // People.
//        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
//        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
//        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
//        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");
//
//        // Note that in this example we use custom affinity key for Person objects
//        // to ensure that all persons are collocated with their organizations.
//        colPersonCache.put(p1.key(), p1);
//        colPersonCache.put(p2.key(), p2);
//        colPersonCache.put(p3.key(), p3);
//        colPersonCache.put(p4.key(), p4);
//
//        // These Person objects are not collocated with their organizations.
//        personCache.put(p1.id, p1);
//        personCache.put(p2.id, p2);
//        personCache.put(p3.id, p3);
//        personCache.put(p4.id, p4);

        Person person1 = null;
        Person person2 = null;
        for(int i = 1; i < 10000; i++) {
            person1 = new Person(org1, "John", "Doe", 1000*i, "John Doe has Master Degree.");
            person2 = new Person(org2, "Jane", "Smith", 1000*i, "Jane Doe has Bachelor Degree.");

            colPersonCache.put(person1.key(), person1);
            colPersonCache.put(person2.key(), person2);
//            personCache.put(person1.id, person1);
//            personCache.put(person2.id, person2);
        }

        initflag.set(true);

        return "all executed.";
    }

    /**并置查询*/
    private void sqlQueryWithJoin() {
        IgniteCache<AffinityKey<Long>, Person> cache = ignite.cache("CollocatedPersons");
//        ignite.cluster().forRemotes().ignite().cache("CollocatedPersons").lock(1);

        /**
         *关联的缓存需要指定模式名(缓存名)
         * 例如下面这个sql关联Organization的时候需要加上Organizations作为前缀
         * 而Person的缓存Persons会作为默认模式名，所以不需要额外指定
         *
         */
        String joinSql = "from Person, \"Organizations\".Organization as org " +
                "where Person.orgId = org.id " +
                "and Person.salary = ?" +
                "and lower(org.name) = lower(?)";

        int i = IntStream.range(1, 101).skip((int)(100*Math.random())).findFirst().getAsInt();

        long stime = System.nanoTime();
        List<Cache.Entry<AffinityKey<Long>, Person>> result = cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, joinSql).
                setArgs(1000*i, "ApacheIgnite")).getAll();
//        print("Following people are 'ApacheIgnite' employees: ",
//                cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, joinSql).
//                        setArgs("ApacheIgnite")).getAll());
        System.out.format("testWithAfty cost time [%s].\n", (System.nanoTime() - stime)/1000000.00);

        print("Following people are 'ApacheIgnite' employees: ", result);

//        print("Following people are 'Other' employees: ",
//                cache.query(new SqlQuery<AffinityKey<Long>, Person>(Person.class, joinSql).
//                        setArgs("Other")).getAll());
    }

    /**非并置查询*/
    private void sqlQueryWithDistributedJoin() {
        IgniteCache<Long, Person> cache = ignite.cache("Persons");
        String joinSql =
                "from Person, \"Organizations\".Organization as org " +
                        "where Person.orgId = org.id " +
                        "and Person.salary = ?" +
                        "and lower(org.name) = lower(?)";
        SqlQuery<Long, Person> qry = new SqlQuery<>(Person.class, joinSql);

        int i = IntStream.range(1, 101).skip((int)(100*Math.random())).findFirst().getAsInt();

        /**
         * 启用非并置的分布式关联
         * 查询映射的节点就会从远程节点通过发送广播或者单播请求的方式获取缺失的数据（本地不存在的数据）
         */
        qry.setDistributedJoins(true);

        long stime =System.nanoTime();
        List<Cache.Entry<Long, Person>> result =  cache.query(qry.setArgs(1000*i, "ApacheIgnite")).getAll();
//        print("Following people are 'ApacheIgnite' employees (distributed join): ", cache.query(qry.setArgs("ApacheIgnite", i*1000)).getAll());
        System.out.format("testNoAfty cost time [%s].\n", (System.nanoTime() - stime)/1000000.00);
        print("Following people are 'ApacheIgnite' employees (distributed join): ", result);
//        print("Following people are 'Other' employees (distributed join): ", cache.query(qry.setArgs("Other")).getAll());
    }

    private static void print(String msg, Iterable<?> col) {
        System.out.println(">>> " + msg);
        System.out.println(">>> " + col);

    }

}