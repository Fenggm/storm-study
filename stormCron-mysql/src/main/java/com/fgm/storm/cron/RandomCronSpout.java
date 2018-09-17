package com.fgm.storm.cron;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 *
 * @Auther: fgm
 */
public class RandomCronSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] arrays;
    private Random random;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.arrays = new String[]{"tom 24","jerry 28","tim 25"};
        random = new Random();
    }

    @Override
    public void nextTuple() {
        //不带上任何的msgID，表示我们不启用ack机制
        collector.emit(new Values(arrays[random.nextInt(arrays.length)]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("randomLines"));
    }
}
