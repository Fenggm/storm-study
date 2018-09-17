package com.fgm.storm.stormack;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 *
 * @Auther: fgm
 */
public class SplitBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String lines = input.getStringByField("lines");
        collector.emit(input,new Values(lines));
        //回调我们的ack
        collector.ack(input);
        //如果消息处理失败，这里我们可以显示的调用fail方法，表示我们的消息处理失败了，那么spout当中也会重新出发fail方法
        // collector.fail(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("bolt2"));
    }
}
