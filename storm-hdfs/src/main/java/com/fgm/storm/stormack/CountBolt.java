package com.fgm.storm.stormack;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 *Bolt的开发一般应继承baseBasicBolt,这个类会自动调用ack或者fail方法
 * @Auther: fgm
 */
public class CountBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String bolt2 = input.getStringByField("bolt2");
            System.out.println("我们最后接收到的数据为" + bolt2);
            //sleep 40s,让消息超时,
            Thread.sleep(40000);
            collector.ack(input);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
