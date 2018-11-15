package org.cord.ignite.servicegrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.ServiceResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: cord
 * @date: 2018/11/13 23:32
 * 服务网格
 * http://localhost:8080/servicegrid/test1
 */
@RestController
@RequestMapping("/servicegrid")
public class ServiceGridController {

    @Autowired
    private Ignite ignite;

    private AtomicBoolean flag = new AtomicBoolean(false);

    @GetMapping("/test1")
    public String test1() {
        init();

        //分布式计算如果不指定集群组的话则会传播到所有节点
        IgniteCompute compute = ignite.compute(ignite.cluster().forAttribute("service.node", true));
//        IgniteCompute compute = ignite.compute(); //未部署服务的节点会抛出空指针
        compute.run(new IgniteRunnable() {
            @ServiceResource(serviceName = "myCounterService", proxySticky = false) //非粘性代理
            private MyCounterService counterService;


            @Override
            public void run() {
                int newValue = counterService.increment();
                System.out.println("Incremented value : " + newValue);
            }
        });

        return "all executed.";
    }

    private void init() {
        if(flag.get()) {
            return;
        }
//        ClusterGroup group =  ignite.cluster().forCacheNodes("myCounterCache");
        ClusterGroup group =  ignite.cluster().forLocal();

        IgniteServices svcs = ignite.services(group);

//        svcs.deployClusterSingleton("myCounterService", new MyCounterServiceImpl());
//        svcs.deployNodeSingleton("myCounterService", new MyCounterServiceImpl());

    }
}
