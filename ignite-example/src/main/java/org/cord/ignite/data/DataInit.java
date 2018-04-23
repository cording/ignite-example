package org.cord.ignite.data;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.cord.ignite.initial.CacheKeyConstant;
import org.cord.ignite.mode.Organization;
import org.cord.ignite.mode.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by cord on 2018/4/11.
 * 数据初始化
 */
@Component
public class DataInit {

    @Autowired
    private Ignite ignite;

    public void dataInit(){
        personInit();
    }

    private void personInit(){
        IgniteDataStreamer<Long, Person> streamer = ignite.dataStreamer(CacheKeyConstant.PERSON);
        IgniteAtomicSequence sequence = ignite.atomicSequence("personPk", 0, true);
        Organization org = new Organization("Test");
        for (int i = 0; i < 1000 ; i++) {
            streamer.addData(sequence.incrementAndGet(), new Person(org, "cord", "", Double.valueOf(i),""));
        }
        streamer.flush();

    }
}
