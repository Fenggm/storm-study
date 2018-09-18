package com.fgm.storm.logmonitor;

import com.fgm.storm.utils.CommonUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/18 14:54
 */
public class NotifyBolt  extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String rules = input.getStringByField("rules");
        //我们需要的那一条数据
        String matchMsg = input.getStringByField("matchMsg");
        CommonUtils.notifyPeople(rules,matchMsg);
        collector.emit(new Values(rules,matchMsg));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("notifyRule","notifyMsg"));
    }
}
