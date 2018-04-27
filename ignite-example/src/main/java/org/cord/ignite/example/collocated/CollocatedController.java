package org.cord.ignite.example.collocated;

import org.apache.catalina.Cluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Created by cord on 2018/4/27.
 * 多cache并置的实现以及查询
 * http://localhost:8080/collocate/query
 * http://localhost:8080/collocate/verify
 */
@Controller
@RequestMapping("/collocate")
public class CollocatedController {

    @Autowired
    private Ignite ignite;

    private static AtomicBoolean init = new AtomicBoolean(false);

    /**
     * 验证并置是否生效的方式
     * 具体方式是检测并置键是否在相同节点
     */
    @RequestMapping("/verify")
    public @ResponseBody
    String verifyCollocate(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if(!init.get()){
            init();
        }

        Affinity<Long> affinityX = ignite.affinity("X");
        Affinity<Long> affinityY = ignite.affinity("Y");
        Affinity<Long> affinityZ = ignite.affinity("Z");

        for (long i = 0; i < 100; i++) {
            ClusterNode nodeX = affinityX.mapKeyToNode(i);
            ClusterNode nodeY = affinityY.mapKeyToNode(i);
            ClusterNode nodeZ = affinityZ.mapKeyToNode(i);

            if(nodeX.id() != nodeY.id() || nodeY.id() != nodeZ.id() || nodeX.id() != nodeZ.id()){
                throw new Exception("cache collocated is error!");
            }
        }
        System.out.println("cache collocated is right!");

        return "all executed.";
    }

    @RequestMapping("/query")
    public @ResponseBody
    String query(HttpServletRequest request, HttpServletResponse response){
        if(!init.get()){
            init();
        }
        IgniteCache<Long, X> xc = ignite.cache("X");
        IgniteCache<AffinityKey<Long>, Y> yc = ignite.cache("Y");
        IgniteCache<AffinityKey<Long>, Z> zc = ignite.cache("Z");

        String sql1 = "from Y,\"X\".X " +
                "where Y.XId = X.id " +
                "and Y.info = ?";
        String sql2 = "from Z,\"Y\".Y " +
                "where Z.YId = Y.id " +
                "and Z.info = ?";
        String sql3 = "from Z,\"Y\".Y,\"X\".X " +
                "where Z.YId = Y.id and Y.XId = X.id " +
                "and Z.info = ?";

        int i = IntStream.range(1, 100).skip((int)(100*Math.random())).findFirst().getAsInt();

        System.out.println("query X and Y:");
        System.out.println(yc.query(new SqlQuery<AffinityKey<Long>, Y>(Y.class, sql1).setArgs(i)).getAll());
        System.out.println("**************************************************************************************");

        System.out.println("query Y and Z:");
        System.out.println(zc.query(new SqlQuery<AffinityKey<Long>, Z>(Z.class, sql2).setArgs(i)).getAll());
        System.out.println("**************************************************************************************");

        System.out.println("query X and Y and Z:");
        System.out.println(zc.query(new SqlQuery<AffinityKey<Long>, Z>(Z.class, sql3).setArgs(i)).getAll());
        System.out.println("**************************************************************************************");

        return "all executed.";
    }

    private String init(){
        if(init.get()){
            return "already execute init.";
        }

        CacheConfiguration<Long, X>  xcf = new CacheConfiguration<Long, X>("X")
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Long.class, X.class);
        CacheConfiguration<AffinityKey<Long>, Y>  ycf = new CacheConfiguration<AffinityKey<Long>, Y>("Y")
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Affinity.class, Y.class);
        CacheConfiguration<AffinityKey<Long>, Z>  zcf = new CacheConfiguration<AffinityKey<Long>, Z>("Z")
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Affinity.class, Z.class);

        ignite.destroyCache("X");
        ignite.destroyCache("Y");
        ignite.destroyCache("Z");
        ignite.getOrCreateCache(xcf);
        ignite.getOrCreateCache(ycf);
        ignite.getOrCreateCache(zcf);

        IgniteCache<Long, X> xc = ignite.cache("X");
        IgniteCache<AffinityKey<Long>, Y> yc = ignite.cache("Y");
        IgniteCache<AffinityKey<Long>, Z> zc = ignite.cache("Z");

        Y y;
        Z z;
        for (long i = 0; i < 100; i++) {
            xc.put(i, new X(i, String.valueOf(i)));
            y = new Y(i, String.valueOf(i), i);
            yc.put(y.key(), y);
            z = new Z(i, String.valueOf(i), i);
            zc.put(z.key(), z);
        }

        init.set(true);
        return "all executed.";
    }
}
