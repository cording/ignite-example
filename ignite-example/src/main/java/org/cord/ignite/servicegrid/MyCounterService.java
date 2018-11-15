package org.cord.ignite.servicegrid;

import javax.cache.CacheException;

/**
 * @author: cord
 * @date: 2018/11/13 23:28
 */
public interface MyCounterService {
    /**
     * Increment counter value and return the new value.
     */
    int increment() throws CacheException;
    /**
     * Get current counter value.
     */
    int get() throws CacheException;
}
