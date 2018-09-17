package com.fgm.storm.cron;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 *实现我们定时任务的功能,每5s打印一次当前时间
 * @Auther: fgm
 */
public class CronBolt extends BaseBasicBolt {

    /**
     * 配置定时时间5s
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        //设置我们的storm程序每隔5s发送一个系统的tuple
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,5);
        return config;
    }

    private SimpleDateFormat simpleDateFormat;

    /**
     * 格式化日期
     * @param stormConf
     * @param context
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 接收上游spout发送过来的tuple，
     * 如果让我们的storm的系统，每隔五秒钟发送一个系统的tuple过来，也会被execute方法执行，那么我们
     * 就可以实现定时的任务的功能
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //判断我们发送过来的数据是我们系统发送的tupel
        if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
            System.out.println("当前时间为"+simpleDateFormat.format(new Date()));
        }else{
            //表明是我们上游发送过来的tuple
            String randomLines = input.getStringByField("randomLines");
            //按照空格进行切分，得到两个字段，一个是name，一个是age
            String[] split = randomLines.split(" ");

            //这里往外发的字段，个数要与我们mysql数据库字段个数保持一致，类型也要保持一致
            //userId    name   age
            collector.emit(new Values(null,split[0],Integer.parseInt(split[1])));
        }
    }

    /**
     * 往下游发送我们声明的字段
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //注意这个声明的字段的个数,要与我们的发送的字段个数保持一致
        declarer.declare(new Fields("userId","name","age"));

    }
}
