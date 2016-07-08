package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_41430ybcix_";
    public static String prex_taobao = "platformTaobao_41430ybcix_";
    public static String prex_ratio = "ratio_41430ybcix_"; 


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    //public static String RocketMQAddress = "127.0.0.1:9876";
    public static String JstormTopologyName = "41430ybcix";
    public static String MetaConsumerGroup = "41430ybcix";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "10.101.72.127:5198";
    //public static String TairSalveConfigServer = "xxx";
    public static String TairGroup = "group_tianchi";//test
    public static Integer TairNamespace = 13202;
}
