package org.cord.ignite.servicegrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.springframework.stereotype.Component;

import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;

/**
 * @author: cord
 * @date: 2018/11/13 23:28
 */
@Component
public class MyCounterServiceImpl implements Service, MyCounterService {
    /** Auto-injected instance of Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;
    /** Distributed cache used to store counters. */
    private IgniteCache<String, Integer> cache;
    /** Service name. */
    private String svcName;
    /**
     * Service initialization.
     */
    @Override public void init(ServiceContext ctx) {
        // Pre-configured cache to store counters.
        cache = ignite.cache("myCounterCache");
        svcName = ctx.name();
        System.out.println("Service was initialized: " + svcName);
    }
    /**
     * Cancel this service.
     */
    @Override public void cancel(ServiceContext ctx) {
        // Remove counter from cache.
        cache.remove(svcName);
        System.out.println("Service was cancelled: " + svcName);
    }
    /**
     * Start service execution.
     */
    @Override public void execute(ServiceContext ctx) {
        // Since our service is simply represented by a counter
        // value stored in cache, there is nothing we need
        // to do in order to start it up.
        System.out.println("Executing distributed service: " + svcName);
    }
    @Override public int get() throws CacheException {
        Integer i = cache.get(svcName);
        return i == null ? 0 : i;
    }
    @Override public int increment() throws CacheException {
        return cache.invoke(svcName, new CounterEntryProcessor());
    }
    /**
     * Entry processor which atomically increments value currently stored in cache.
     */
    private static class CounterEntryProcessor implements EntryProcessor<String, Integer, Integer> {
        @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {
            int newVal = e.exists() ? e.getValue() + 1 : 1;
            // Update cache.
            e.setValue(newVal);
            return newVal;
        }
    }
}
