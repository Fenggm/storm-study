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
 * @Date: 2018/9/18 14:45
 */
public class ProcessBolt extends BaseBasicBolt {

    /**
     * 获取我们的数据与我们的每个规则进行匹配,如果匹配上就证明是我们想要的数据
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String errorMsg = input.getStringByField("errorMsg");
        //校验这条数据是不是我们关心的数据
        String[] split = errorMsg.split("\001");
        String rules = CommonUtils.checkRules(split[0], split[1]);
        //如果rules返回的是"",表明没有匹配上任何数据,不是我们要的数据
        if (null!=rules && rules!=""){
            //是我们需要的数据,继续往下发送
            collector.emit(new Values(rules, errorMsg));

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rules","matchMsg"));
    }
}
