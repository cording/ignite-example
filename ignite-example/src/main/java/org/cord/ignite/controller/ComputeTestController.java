package org.cord.ignite.controller;

import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.cord.ignite.data.domain.Student;
import org.cord.ignite.initial.CacheKeyConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.cache.Cache;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author: cord
 * @date: 2018/8/22 22:50
 * http://localhost:8080/broadcast
 * http://localhost:8080/call
 * http://localhost:8080/apply
 * http://localhost:8080/affinity
 * http://localhost:8080/affinity2
 * http://localhost:8080/taskMap
 * http://localhost:8080/taskSplit
 */
@RestController
public class ComputeTestController {

    @Autowired
    private Ignite ignite;

    @Autowired
    private IgniteConfiguration igniteCfg;

    /** broadCast测试*/
    @RequestMapping("/broadcast")
    String broadcastTest(HttpServletRequest request, HttpServletResponse response) {
//        IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());  //只传播远程节点
        IgniteCompute compute = ignite.compute();
        compute.broadcast(() -> System.out.println("Hello Node: " + ignite.cluster().localNode().id()));
        return "all executed.";
    }

    /** call和run测试 */
    @RequestMapping("/call")
    public @ResponseBody
    String callTest(HttpServletRequest request, HttpServletResponse response) {
        Collection<IgniteCallable<Integer>> calls = new ArrayList<>();

        /** call */
        System.out.println("-----------call-----------");
        for(String word : "How many characters".split(" ")) {
            calls.add(word::length);
//            calls.add(() -> word.length());
        }
        Collection<Integer> res = ignite.compute().call(calls);
        int total = res.stream().mapToInt(Integer::intValue).sum();
        System.out.println(String.format("the total lengths of all words is [%s].", total));

        /** run */
        System.out.println("-----------run-----------");
        for (String word : "Print words on different cluster nodes".split(" ")) {
            ignite.compute().run(() -> System.out.println(word));
        }

        /** async call */
        System.out.println("-----------async call-----------");
        IgniteCompute asyncCompute =  ignite.compute().withAsync();
        asyncCompute.call(calls);
        asyncCompute.future().listen(fut -> {
            Collection<Integer> result = (Collection<Integer>)fut.get();
            int t = result.stream().mapToInt(Integer::intValue).sum();
            System.out.println("Total number of characters: " + total);
        });

        /** async run */
        System.out.println("-----------async run-----------");
        Collection<ComputeTaskFuture<?>> futs = new ArrayList<>();
        asyncCompute = ignite.compute().withAsync();
        for (String word : "Print words on different cluster nodes".split(" ")) {
            asyncCompute.run(() -> System.out.println(word));
            futs.add(asyncCompute.future());
        }
        futs.stream().forEach(ComputeTaskFuture::get);

        return "all executed.";
    }

    /** apply测试 */
    @RequestMapping("/apply")
    public @ResponseBody
    String applyTest(HttpServletRequest request, HttpServletResponse response) {
        /** apply */
        System.out.println("-----------apply-----------");
        IgniteCompute compute = ignite.compute();
        Collection<Integer> res = compute.apply(
                String::length,
                Arrays.asList("How many characters".split(" "))
        );
        int total = res.stream().mapToInt(Integer::intValue).sum();
        System.out.println(String.format("the total lengths of all words is [%s].", total));

        /** async apply */
        IgniteCompute asyncCompute = ignite.compute().withAsync();
        res = asyncCompute.apply(
                String::length,
                Arrays.asList("How many characters".split(" "))
        );
        asyncCompute.future().listen(fut -> {
            int t = ((Collection<Integer>)fut.get()).stream().mapToInt(Integer::intValue).sum();
            System.out.println(String.format("Total number of characters: " + total));
        });

        return "all executed.";
    }

    /**并置计算测试*/
    @RequestMapping("/affinity")
    public @ResponseBody
    String affinityTest(HttpServletRequest request, HttpServletResponse response) {

        /** affinityRun call */
        System.out.println("-----------affinityRun call-----------");
        IgniteCompute compute = ignite.compute();
//        IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());
        for(int key = 0; key < 100; key++) {
//            final long k = key;
            //生成随机k值
            final long k = IntStream.generate(() -> (int)(System.nanoTime() % 100)).limit(1).findFirst().getAsInt();
            compute.affinityRun(CacheKeyConstant.STUDENT, k, () -> {
                IgniteCache<Long, BinaryObject> cache = ignite.cache(CacheKeyConstant.STUDENT).withKeepBinary();
                BinaryObject obj = cache.get(k);
                if(obj!=null) {
                    System.out.println(String.format("Co-located[key= %s, value= %s]", k, obj.<String>field("name")));
                }
            });
        }

        IgniteCache<Long, BinaryObject> cache = ignite.cache(CacheKeyConstant.STUDENT).withKeepBinary();
        cache.forEach(lo -> compute.affinityRun(CacheKeyConstant.STUDENT, lo.getKey(), () -> {
            System.out.println(lo.getValue().<String>field("name"));
        }));

        return "all executed.";
    }

    /**分区模式测试并置计算*/
    @RequestMapping("/affinity2")
    public @ResponseBody
    String affinityTest2(HttpServletRequest request, HttpServletResponse response) {
        System.out.println("-----------affinityRun run-----------");

        IgniteCompute compute = ignite.compute();
        for(int key = 1; key <= 50; key++) {
            long k = key;
            compute.affinityRun(CacheKeyConstant.STUDENT, k, () -> {
                IgniteCache<Long, Student> cache = ignite.cache(CacheKeyConstant.STUDENT);
                List<List<?>> res = cache.query(new SqlFieldsQuery("select _VAL,name from \"student\".student")).getAll();
                System.out.format("The name is %s.\n", res.get(0).get(0));
            });
        }

        for(int key = 1; key <= 50; key++) {
            long k = key;
            compute.affinityRun("Test", k, () -> {
                IgniteCache<Long, Student> cache = ignite.cache(CacheKeyConstant.STUDENT);
                Student stu = cache.get(k);
                if(stu!=null) {
                    System.out.println(String.format("Co-located[key= %s, value= %s]", k, stu.getName()));
                }
            });
        }
        return "all executed.";
    }

    /**ComputeTaskAdapter*/
    @RequestMapping("/taskMap")
    public @ResponseBody
    String taskMapTest(HttpServletRequest request, HttpServletResponse response) {
        /**ComputeTaskMap*/
        int cnt = ignite.compute().execute(MapExampleCharacterCountTask.class, "Hello Ignite Enable World!");

        System.out.println(String.format(">>> Total number of characters in the phrase is %s.", cnt));

        return "all executed.";
    }

    private static class MapExampleCharacterCountTask extends ComputeTaskAdapter<String, Integer> {
        /**节点映射*/
        @Override
        public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, String arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> map = new HashMap<>();
            Iterator<ClusterNode> it = nodes.iterator();
            for (final String word : arg.split(" ")) {
                // If we used all nodes, restart the iterator.
                if (!it.hasNext()) {
                    it = nodes.iterator();
                }
                ClusterNode node = it.next();
                map.put(new ComputeJobAdapter() {
                    @Override
                    public Object execute() throws IgniteException {
                        System.out.println("-------------------------------------");
                        System.out.println(String.format(">>> Printing [%s] on this node from ignite job.", word));
                        return word.length();
                    }
                }, node);
            }
            return map;
        }
        /**结果汇总*/
        @Override
        public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            int sum = 0;
            for (ComputeJobResult res : results) {
                sum += res.<Integer>getData();
            }
            return sum;
        }
    }

    /**ComputeTaskSplitAdapter*/
    @RequestMapping("/taskSplit")
    public @ResponseBody
    String taskSplitTest(HttpServletRequest request, HttpServletResponse response) {
        /**ComputeTaskSplitAdapter(自动映射) */
        int result = ignite.compute().execute(SplitExampleDistributedCompute.class, null);
        System.out.println(String.format(">>> result: [%s]", result));

        return "all executed.";
    }

    private static class SplitExampleDistributedCompute extends ComputeTaskSplitAdapter<String, Integer> {

        @Override
        protected Collection<? extends ComputeJob> split(int gridSize, String arg) throws IgniteException {
            Collection<ComputeJob> jobs = new LinkedList<>();
            jobs.add(new ComputeJobAdapter() {
                @Override
                public Object execute() throws IgniteException {
//                    IgniteCache<Long, Student> cache = Ignition.ignite().cache(CacheKeyConstant.STUDENT);
                    IgniteCache<Long, BinaryObject> cache = Ignition.ignite().cache(CacheKeyConstant.STUDENT).withKeepBinary();
                    /**普通查询*/
                    String sql_query = "name = ? and email = ?";
//                    SqlQuery<Long, Student> cSqlQuery = new SqlQuery<>(Student.class, sql_query);
                    SqlQuery<Long, BinaryObject> cSqlQuery = new SqlQuery<>(Student.class, sql_query);
                    cSqlQuery.setReplicatedOnly(true).setArgs("student_54", "student_54gmail.com");
//                  List<Cache.Entry<Long, Student>> result = cache.query(cSqlQuery).getAll();
                    List<Cache.Entry<Long, BinaryObject>> result = cache.query(cSqlQuery).getAll();
                    System.out.println("--------------------");
                    result.stream().map(x -> {
                        Integer studId = x.getValue().field("studId");
                        String name = x.getValue().field("name");
                        return String.format("name=[%s], studId=[%s].", name, studId);
                    }).forEach(System.out::println);
                    System.out.println(String.format("the query size is [%s].", result.size()));
                    return result.size();
                }
            });
            return jobs;
        }

        @Override
        public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            int sum = results.stream().mapToInt(x -> x.<Integer>getData()).sum();
            return sum;
        }
    }
}
