package com.fgm.storm.stormToHdfs;

import com.fgm.storm.domain.PaymentInfo;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 *
 * @Auther: fgm
 */
public class RandomOrderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        PaymentInfo paymentInfo = new PaymentInfo();
        //获取一个json格式的字符串
        String random = paymentInfo.random();

        //如果发送消息的时候,带上了msgId,就表明我们开启了storm的ack机制.
        collector.emit(new Values(random), random);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //将数据传递给Bolt
        declarer.declare(new Fields("randomOrder"));
    }

    /**
     * 消息失败的时候会调用这个方法(默认30s没有消掉就算超时失败)
     * 我们这里指定的msgId的，就是我们的消息本身，有一个好处就是获取到了我们的msgI的，
     * 就能获取到我们失败的消息
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        collector.emit(new Values(msgId), msgId);

    }

    /**
     * 消息处理成功的时候会回调这个方法
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }
}
