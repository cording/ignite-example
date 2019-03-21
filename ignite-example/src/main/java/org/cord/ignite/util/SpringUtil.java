package org.cord.ignite.util;

/**
 * @author: cord
 * @date: 2019/3/21 21:28
 */

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringUtil implements ApplicationContextAware {

    private static ApplicationContext ctx;

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {

        if (SpringUtil.ctx == null) {
            SpringUtil.ctx = ctx;
        }
    }

    public static ApplicationContext getApplicationContext() {
        return ctx;
    }

    /**
     * 通过 name 获取 Bean
     */
    public static Object getBean(String name) {
        return ctx.getBean(name);
    }

    /**
     * 通过 class 获取 Bean
     */
    public static <T> T getBean(Class<T> clazz) {
        return ctx.getBean(clazz);
    }

    /**
     * 通过 name 和 class 获取 Bean
     */
    public static <T> T getBean(String name, Class<T> clazz) {
        return ctx.getBean(name, clazz);
    }

}
