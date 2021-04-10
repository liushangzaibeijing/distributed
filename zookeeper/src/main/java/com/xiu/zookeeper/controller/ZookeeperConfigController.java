package com.xiu.zookeeper.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

/**
 * @ClassName ZookeeperConfigController
 * @Desc TODO
 * @Author xieqx
 * @Date 2021/3/12 15:52
 **/
@Controller
public class ZookeeperConfigController {

    @Value("${server.port}")
    public static String port ;

    public String getPort(){
        return port;
    }
}
