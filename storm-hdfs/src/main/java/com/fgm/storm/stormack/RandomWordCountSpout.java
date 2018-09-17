package com.fgm.storm.stormack;

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
public class RandomWordCountSpout  extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random random;
    private String[] arrays;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
        random=new Random();
        arrays=new String[]{"hello world","hadoop hive","storm ack"};

    }

    @Override
    public void nextTuple() {
        String line = arrays[random.nextInt(arrays.length)];
        collector.emit(new Values(line),line);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lines"));
    }

    /**
     * 如果成功会调用这个ack方法
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        System.out.println("this is the method of success ack"+msgId.toString());
    }

    /**
     * 如果超时,会调用这个fail方法
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        collector.emit(new Values(msgId),msgId);
        System.out.println("this is the method of fail "+msgId.toString());
    }
}
