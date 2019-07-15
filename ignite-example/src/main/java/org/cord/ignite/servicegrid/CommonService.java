package org.cord.ignite.servicegrid;

/**
 * 通用服务接口
 * 相当于一个客户端集成jetty，对外提供http接口，用于间接调用服务网格，
 * 并且统一返回json字符串
 */
public interface CommonService {

    String execute() throws Exception;

}