package org.cord.ignite.data.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.cord.ignite.data.dao.StudentDao;
import org.cord.ignite.data.domain.Student;
import org.cord.ignite.initial.CacheKeyConstant;
import org.cord.ignite.mode.Organization;
import org.cord.ignite.mode.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by cord on 2018/4/11.
 * 数据加载
 */
@Component
public class DataLoader {

    private static final Logger log = LogManager.getLogger(DataLoader.class);

    @Autowired
    private StudentDao studentDao;

    @Autowired
    private Ignite ignite;

    /**导入数据到ignite*/
    public void loadData(){
        //查询student集合
        List<Student> result = studentDao.findAllStudents();
        //分布式id生成器
        IgniteAtomicSequence sequence = ignite.atomicSequence("studentPk", 0, true);
        //根据缓存名获取流处理器，并往流处理器中添加数据
        try(IgniteDataStreamer<Long, Student> streamer = ignite.dataStreamer(CacheKeyConstant.STUDENT)) {
            result.stream().forEach(r -> streamer.addData(sequence.incrementAndGet(), r));
            //将流里面的剩余数据压进ignite
            streamer.flush();
        }
    }
}
