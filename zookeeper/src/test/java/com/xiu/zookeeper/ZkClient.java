package com.xiu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.TestPropertySource;

import javax.annotation.PostConstruct;
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
    private static final int sessionTimeOut = 10*1000;
    //创建同步节点的前缀
    private static final String SYNC_NODE_PREFIX = "/xiu-sync-zk-";
    //创建异步节点的前缀
    private static final String ASYNC_NODE_PREFIX = "/xiu-async-zk-";

    //用于创建连接的异步调用阻塞
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper;

    //异步回调通知 比如建立连接过程
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

    //1、zookeeper 建立连接是异步的 会直接返回 可以使用CountDownLatch进行阻塞通过Watch监控对象来获取其是否建立成功
    /**
     * 创建zookeeper 连接api 相关参数
     * connetionString: 连接ip和host组成的字符串 ip:host
     * sessionTimeout: 建立连接的绘画超时时间，超过该时间需要重新建立连接
     * Watcher: zk所有事件发生的回调函数接口 用于通知连接建立和节点变化
     * canBeReadOnly: 对zk上节点读写权限 true 不能创建节点 只能获取节点数据
     */
    //创建连接
    @BeforeAll
    public static void getConnection(){
        try {
            if(zooKeeper==null){
                zooKeeper = new ZooKeeper(connetionString,sessionTimeOut,new ZkClient(),false);
                log.info("zk 开始了异步连接，连接状态：{}",zooKeeper.getState());
                countDownLatch.await();
                //使用sessionId和sessionPasswd 复用会话
                long sessionId = zooKeeper.getSessionId();
                byte[] sessionPasswd = zooKeeper.getSessionPasswd();
//                ZooKeeper repeatZk = new ZooKeeper(connetionString,sessionTimeOut,new ZkClient(),sessionId,sessionPasswd,false);
//                log.info("复用zk:{}",repeatZk.hashCode());
//                log.info("初始zk:{}",zooKeeper.hashCode());
                //两者不一样
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
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
     */
    //创建同步节点
    @Test
    public  void createSyncNode(){
        try {
            //创建一个顺序持久化节点 节点始终存在  节点权限对所有人开放
            String  persistentNodePath = zooKeeper.create(SYNC_NODE_PREFIX+"create", "persistent".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            //创建一个临时节点 连接关闭该节点会被删除
            String ephemeralNodePath = zooKeeper.create(SYNC_NODE_PREFIX+"temp", "ephemeral".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            //获取节点 的相关性质：路径 数据 节点属性
            log.info("创建同步持久化节点成功。节点路径：{}",persistentNodePath);
            log.info("创建同步临时节点成功。节点路径：{}",ephemeralNodePath);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步常见节点不会直接返回节点信息，而是通过AsyncCallback的StringCallback和Create2Callback回调函数进行创建完成/创建异常的回调通知
     * 两个回调函数区别
     *   StringCallback 上下文对象必须是String类型
     *   Create2Callback 上下文为Object对象且回调函数多了一个节点属性信息Stat对象
     * 回调函数参数：
     *   int rc为服务器的响应码，0表示调用成功，-4表示连接已断开，-110表示指定节点已存在，-112表示会话已过期。
     *   String path调用create方法时传入的path。
     *   Object ctx调用create方法时传入的ctx。
     *   String name创建成功的节点名称。
     */

    //无论同步和异步节点 都无法递归的创建节点，即无法在父节点不存在的情况下创建子节点，节点存在再次创建会抛出NodeExistsException
    //创建异步节点
    @Test
    public  void createASyncNode(){
        //异步节点创建成功后的接口回调函数为StringCallback
        zooKeeper.create(ASYNC_NODE_PREFIX + "create", "persistent".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        log.info("异步创建节点成功节点信息：rc:{},path:{},ctx:{},name:{}",rc,path,ctx,name);
                    }
                },"睡觉");
        //异步节点创建成功后的接口回调函数为Create2Callback 比StringCallBack多了一个节点属性对象Stat
        zooKeeper.create(ASYNC_NODE_PREFIX + "create", "persistent".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, new AsyncCallback.Create2Callback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
                        log.info("异步创建子节点成功节点信息：rc:{},path:{},ctx:{},name:{}",rc,path,ctx,name);
                        log.info("节点属性信息：{}",stat.toString());
                    }
                },"睡觉");

        try {
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    //获取查看同步节点
    //查看所有节点 getChildren(path,watch(boolean or Watch))
    //     boolean watch true使用默认的Zookeeper自带的监听器
    //     Watch watch 注册用于监听节点变化的监视器，对于节点内容变化，节点删除进行监听


    @Test
    public void getNode(){
        try {
            //获取指定路径下的字节点列表 同时添加一个Watch监听指定节点路径的变化
            zooKeeper.getChildren(SYNC_NODE_PREFIX+"create",new ZkClient());

            zooKeeper.create(SYNC_NODE_PREFIX+"create/son2", "sonNew".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            Stat stat = new Stat();
            //同步获取节点数据
            byte[] data = zooKeeper.getData(SYNC_NODE_PREFIX + "create", new ZkClient(), stat);
            zooKeeper.setData(SYNC_NODE_PREFIX+"create","改变节点".getBytes(),-1);
            //异步获取数据
            zooKeeper.getData(SYNC_NODE_PREFIX + "create", new ZkClient(), new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    log.info("获取数据响应码rc：{},path:{},ctx:{},data:{},stat:{}",rc,path,ctx,new String(data),stat);
                }
            },"环境数据对象");
            zooKeeper.delete(SYNC_NODE_PREFIX+"create",-1);

            log.info("节点数据：{},属性信息stat:{}",new String(data),stat);

            Thread.sleep(1000);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新节点数据
     * Zookeeper更新节点数据有版本的概念，为了解决并发修改的问题，更改数据必须带上版本，
     * 如果当前版本数据已经被修改 则该次修改不成功。version=-1表示每次使用最新版本进行更新
     */
    @Test
    public void updateData(){
        try {
            //同步更新节点
            Stat stat = zooKeeper.setData(SYNC_NODE_PREFIX + "create", "修改节点".getBytes(), -1);

            zooKeeper.setData(SYNC_NODE_PREFIX + "create", "修改节点3".getBytes(), stat.getVersion()-1);

            //异步更新节点 且使用旧版本更新（会出现错误）
            zooKeeper.setData(SYNC_NODE_PREFIX + "create", "修改节点2".getBytes(), stat.getVersion()-1, new AsyncCallback.StatCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, Stat stat) {
                            log.info("更新数据响应码rc：{},path:{},ctx:{},stat:{}",rc,path,ctx,stat);
                        }
                    }
                    , "更新数据");

            Thread.sleep(1000);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void searchAllChilderns(){
        //查看根节点下的所有子节点
        this.getChildNode("/");
    }

    private void getChildNode(String nodeName){
        try {
            List<String> childrens = zooKeeper.getChildren(nodeName, false);
            for(String children:childrens){
                if(nodeName.equals("/")){
                    children = nodeName+children;
                }else{
                    children = nodeName+"/"+children;
                }
                System.out.println("当前节点"+children);
                if(zooKeeper.getChildren(children,false).size()>0){
                    getChildNode(children);
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void deleteNode(){

        try {
            //同步删除节点 -1删除最新版本
            zooKeeper.delete(ASYNC_NODE_PREFIX+"create",-1);

            //异步删除节点
            zooKeeper.delete(ASYNC_NODE_PREFIX + "create", -1, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    log.info("更新数据响应码rc：{},path:{},ctx:{}",rc,path,ctx);
                }
            },"上下文");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }



        //递归删除节点
        this.deleteRootCase(SYNC_NODE_PREFIX+"create");
    }


    private void deleteRootCase(String nodeName){
        try {
            List<String> childrens = zooKeeper.getChildren(nodeName, new ZkClient());
            for(String children:childrens){
                if(nodeName.equals("/")){
                    children = nodeName+children;
                }else{
                    children = nodeName+"/"+children;
                }
                System.out.println("当前节点"+children);
                int size = zooKeeper.getChildren(children, false).size();

                if(size > 0){
                    deleteRootCase(children);
                }
                if(size == 0){
                    zooKeeper.delete(children,-1);
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }




    //检查节点是否存在
    //监听节点变化
    //
}
