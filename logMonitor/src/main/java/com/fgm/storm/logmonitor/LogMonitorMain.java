package com.fgm.storm.logmonitor;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/18 14:24
 */
public class LogMonitorMain {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        KafkaSpoutConfig.Builder<String, String> kafkaSpoutConfigBuilder = KafkaSpoutConfig.builder("node01:9092,node02:9092,node03:9092", "logmonitor");
        kafkaSpoutConfigBuilder.setGroupId("logMonitorGroup");
        kafkaSpoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);

        KafkaSpoutConfig<String, String> build = kafkaSpoutConfigBuilder.build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(build);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout",kafkaSpout);
        topologyBuilder.setBolt("crontabBolt", new CrontabBolt()).localOrShuffleGrouping("kafkaSpout");
        topologyBuilder.setBolt("processBolt", new ProcessBolt()).localOrShuffleGrouping("crontabBolt");
        topologyBuilder.setBolt("notifyBolt", new NotifyBolt()).localOrShuffleGrouping("processBolt");
        topologyBuilder.setBolt("saveDbBolt", new SaveDbBolt()).localOrShuffleGrouping("notifyBolt");

        Config config = new Config();

        if (null!=args && args.length>0){
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("logMonitor",config,topologyBuilder.createTopology());
        }
    }
}
