package com.xiu.zookeeper.configcenter.controller;

import com.xiu.zookeeper.configcenter.configuration.PayMoneyProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("zk")
public class ZkConfigController {
    @Autowired
    private PayMoneyProperties payMoneyProperties ;

    @RequestMapping("/pay/money")
    public  Object getZkConfig(){
        String money ="项目顺利上线，老板开始发奖金：";
        return money + payMoneyProperties.getMoney();
    }
}
