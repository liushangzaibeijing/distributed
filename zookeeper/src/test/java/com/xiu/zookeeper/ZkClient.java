package com.xiu.zookeeper;

import org.apache.zookeeper.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @ClassName ZkClient
 * @Desc zookeeper
 * @Author xieqx
 * @Date 2021/2/26 16:02
 **/
public class ZkClient implements Watcher {
    //连接zkServer的 host:port
    private static final String connetionString = "182.92.189.235:2182";
    //zk服务连接的session超时时间
    private static final int sessionTimeOut = 1000;
    //创建节点的前缀
    private static final String PREFIX = "/xiu-zk";

    private static ZooKeeper zooKeeper = null;

    public static void main(String[] args) throws IOException {
        //getConnection();
        createNode();
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("监听到zk事件:"+event);
    }

    /**
     * 创建zookeeper 连接api 相关参数
     * connetionString: 连接ip和host组成的字符串 ip:host
     * sessionTimeout: 建立连接的绘画超时时间，超过该时间需要重新建立连接
     * Watcher: zk所有事件发生的回调函数接口
     * canBeReadOnly: 对zk上节点读写权限 true 不能创建节点 只能获取节点数据
     */
    //创建连接
    public static  ZooKeeper getConnection(){
        try {
            if(zooKeeper==null){
                zooKeeper = new ZooKeeper(connetionString,sessionTimeOut,new ZkClient(),false);
                System.out.println("zk 建立连接成功，连接状态："+zooKeeper.getState());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zooKeeper;
    }
    /**
     * 创建节点相关参数
     * path：节点对应的路径 （zk是一个基于树结构的文件目录系统 有路径）
     * data: 节点对应的存储数据（byte[]形式）
     * ACLs: access control List 目录节点访问需要相关的权限控制 该参数是对于节点访问的ACL权限控制集合
     * CreateMode:创建节点模式  临时、持久化等
     */
    //创建节点 同步异步
    public static void createNode(){
        ZooKeeper connection = getConnection();
        try {

            String path = zooKeeper.create(PREFIX, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //删除节点 同步异步
    //获取节点 同步异步
    //更新节点 同步异步
    //检查节点是否存在
    //监听节点变化
    //
}
