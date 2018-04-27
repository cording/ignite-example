package org.cord.ignite.example.compute;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * Created by cord on 2018/4/27.
 */
@Component
public class ComputeTask implements ApplicationContextAware{

    private static ApplicationContext CONTEXT;

    public void run(){
        System.out.println("task is running.");
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        CONTEXT = applicationContext;
    }

    public static ComputeTask getBean(){
        return (ComputeTask) CONTEXT.getBean("computeTask");
    }
}
