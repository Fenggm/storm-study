package com.fgm.storm.stormack;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @Auther: fgm
 */
public class WordCountAckMain {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randAck",new RandomWordCountSpout());
        builder.setBolt("splitBolt",new SplitBolt()).localOrShuffleGrouping("randAck");
        builder.setBolt("countBolt",new CountBolt()).localOrShuffleGrouping("splitBolt");

        Config config = new Config();

        if (null!=args && args.length>0){
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("acks",config,builder.createTopology());
        }

    }

}
