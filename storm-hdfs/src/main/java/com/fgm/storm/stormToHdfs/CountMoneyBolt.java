package com.fgm.storm.stormToHdfs;

import com.alibaba.fastjson.JSON;
import com.fgm.storm.domain.PaymentInfo;
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
public class CountMoneyBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //接收上游发送过来的json数据
        String randomOrder = input.getStringByField("randomOrder");
        PaymentInfo paymentInfo = JSON.parseObject(randomOrder, PaymentInfo.class);
        System.out.println(paymentInfo.getPayPrice());
        collector.emit(new Values(randomOrder));

        //如果继承的是BaseRichBolt,开启了ack机制的话，一定要在处理完成之后调用一下ack方法，表明我们消息正常收到并且处理完成了
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("countMoney"));
    }
}
