package org.cord.ignite.example.compute;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by cord on 2018/4/27.
 * 分布式计算
 * http://localhost:8080/compute/affinity
 */
@Controller
@RequestMapping("/compute")
public class ComputeController {

    @Autowired
    private Ignite ignite;

    private static AtomicBoolean init = new AtomicBoolean(false);

    @RequestMapping("/affinity")
    public @ResponseBody
    String affinity(HttpServletRequest request, HttpServletResponse response){
        if(!init.get()){
           init();
        }
        IgniteCache<Long, String> cache = ignite.cache("compute");
        IgniteCompute compute = ignite.compute();

        for (long i = 0; i < 100; i++) {
            compute.affinityRun("compute", i, () -> {
                ComputeTask.getBean().run();
            });
        }

        return "all executed.";
    }


    public void init(){
        CacheConfiguration<Long, String> cf = new CacheConfiguration<Long, String>("compute")
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Long.class, String.class);
        ignite.destroyCache("compute");
        ignite.getOrCreateCache("compute");

        IgniteDataStreamer<Long, String> ids = ignite.dataStreamer("compute");

        for (long i = 0; i < 100; i++) {
            ids.addData(i, String.valueOf(i));
        }
        ids.flush();

        init.set(true);
    }



}
