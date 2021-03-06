package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.race.RaceConfig;



/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {



    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        //conf.setNumWorkers(3);
        int spout_Parallelism_hint = 1;
        int time_Parallelism_hint = 2;
        int buffer_Parallelism_hint = 2;
        int count_Parallelism_hint = 4;
        int ratio_Parallelism_hint = 1;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new TestSpout(), spout_Parallelism_hint);
        builder.setBolt("timestamp", new RaceTimeBolt(), time_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("buffer", new RaceBufferBolt(), buffer_Parallelism_hint).shuffleGrouping("timestamp");
        builder.setBolt("count", new RaceCountBolt(), count_Parallelism_hint).fieldsGrouping("buffer", new Fields("time"));
        builder.setBolt("ratio", new RaceRatioBolt(), ratio_Parallelism_hint).allGrouping("timestamp");
        String topologyName = RaceConfig.JstormTopologyName;
        
      LocalCluster cluster = new LocalCluster();

      //建议加上这行，使得每个bolt/spout的并发度都为1
      //conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);

      //提交拓扑
      cluster.submitTopology(topologyName, conf, builder.createTopology());

      //等待1分钟， 1分钟后会停止拓扑和集群， 视调试情况可增大该数值
      Thread.sleep(60000);        

      //结束拓扑
      cluster.killTopology(topologyName);

      cluster.shutdown();
        try {
            //StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}