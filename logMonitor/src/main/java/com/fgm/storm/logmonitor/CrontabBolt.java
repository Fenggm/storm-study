package com.fgm.storm.logmonitor;

import com.fgm.storm.utils.CommonUtils;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/18 14:21
 */
public class CrontabBolt extends BaseBasicBolt {

    private CommonUtils commonUtils;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        commonUtils=new CommonUtils();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,5);
        return config;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
       if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)&&input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
           //判断是我们系统定时任务发送的tuple,需要在这里定时的更新我们的规则
           commonUtils.monitorRule();
           commonUtils.monitorApp();
           commonUtils.monitorUser();
       }else{
           String string = input.getString(4);
           collector.emit(new Values(string));
       }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("errorMsg"));
    }
}
