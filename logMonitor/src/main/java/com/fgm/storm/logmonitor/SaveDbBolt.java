package com.fgm.storm.logmonitor;

import com.fgm.storm.domain.LogMonitorRuleRecord;
import com.fgm.storm.utils.JdbcUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Date;

/**
 *
 * @Auther: fgm
 * @Date: 2018/9/18 14:57
 */
public class SaveDbBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //获取上游发送过来的数据
        String notifyRule = input.getStringByField("notifyRule");
        String notifyMsg = input.getStringByField("notifyMsg");
        //将我们的异常信息保存起来
        String[] split = notifyMsg.split("\001");
        LogMonitorRuleRecord ruleRecord = new LogMonitorRuleRecord();
        ruleRecord.setAppId(Integer.parseInt(split[0]));
        ruleRecord.setCreateDate(new Date());
        ruleRecord.setIsClose(1);
        ruleRecord.setIsPhone(1);
        ruleRecord.setIsEmail(1);
        ruleRecord.setNoticeInfo(notifyMsg);
        ruleRecord.setRuleId(1);
        ruleRecord.setUpdateDate(new Date());
        JdbcUtils.saveRuleRecord(ruleRecord);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
