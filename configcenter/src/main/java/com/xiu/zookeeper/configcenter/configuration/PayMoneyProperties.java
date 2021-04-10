package com.xiu.zookeeper.configcenter.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

@ConfigurationProperties("company.pay")
@RefreshScope
@Component
public class PayMoneyProperties {
    //key结尾部分，以小数点为间隔
    Integer  money ;

    public Integer getMoney() {
        return money;
    }

    public void setMoney(Integer money) {
        this.money = money;
    }
}
