package com.xiu.zookeeper;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ClassName ZkClient
 * @Desc zookeeper
 * @Author xieqx
 * @Date 2021/2/26 16:02
 **/
public class ZkClient implements Watcher {
    //日志
    private static Logger log = LoggerFactory.getLogger(ZkClient.class);

    //连接zkServer的 host:port 集群以逗号分隔
    private static final String connetionString = "182.92.189.235:2181";
    //zk服务连接的session超时时间
    private static final int sessionTimeOut = 1000;
    //创建同步节点的前缀
    private static final String SYNC_NODE_PREFIX = "/xiu-sync-zk-";
    //创建异步节点的前缀
    private static final String ASYNC_NODE_PREFIX = "/xiu-async-zk-";

    //用于创建连接的异步调用阻塞
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper = null;

    public static void main(String[] args) throws IOException {
        //getConnection();
        createSyncNode();
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("监听到zk事件:"+event);
        log.info("监听到zk事件：{}",event);
        if(event.getState() == Event.KeeperState.SyncConnected){
            //连接已经建立停止阻塞
            countDownLatch.countDown();
            log.info("zk连接已经建立，连接状态：{}",event.getState());
        }
    }

    //1、zookeeper 建立连接是异步的 会直接返回 可以通过Watch监控对象来获取其是否建立成功

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
                log.info("zk 开始了异步连接，连接状态：{}",zooKeeper.getState());
                countDownLatch.await();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return zooKeeper;
    }
    /**
     * 创建同步节点节点相关参数
     * path：节点对应的路径 （zk是一个基于树结构的文件目录系统 有路径）
     * data: 节点对应的存储数据（byte[]形式）
     * ACLs: access control List 目录节点访问需要相关的权限控制 该参数是对于节点访问的ACL权限控制集合
     * CreateMode:创建节点模式  临时、持久化等
     *   PERSISTENT 持久化节点
     *   PERSISTENT_SEQUENTIAL 持久顺序节点
     *   EPHEMERAL 临时节点
     *   EPHEMERAL_SEQUENTIAL 临时自动编号节点
     *   PERSISTENT_WITH_TTL  持久化节点 客户端关闭不会被删除，但是在ttl时间内没有被修改过会被删除
     *   PERSISTENT_SEQUENTIAL_WITH_TTL  持久化顺序节点 作用同 PERSISTENT_WITH_TTL
     *
     *
     */
    //创建节点 同步异步
    public static void createSyncNode(){
        ZooKeeper connection = getConnection();
        try {
            //创建一个持久化节点 节点始终存在
            String  persistentNodePath = zooKeeper.create(SYNC_NODE_PREFIX+"create", "persistent".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //创建一个临时节点 连接关闭该节点会被删除
            String ephemeralNodePath = zooKeeper.create(SYNC_NODE_PREFIX, "ephemeral".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //获取节点 的相关性质：路径 数据 节点属性
            log.info("创建节点成功。节点路径：{}",persistentNodePath);

            //创建一个
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
