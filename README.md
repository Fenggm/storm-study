### storm-hdfs为storm与hdfs的结合使用.
这里主要调用官方的API进行测试.  http://storm.apache.org/releases/1.1.2/storm-hdfs.html
<br>
其中,com.fgm.storm.stormack中为ack机制的测试案例.<br>
通过Ack机制，spout发送出去的每一条消息，都可以确定是被成功处理或失败处理， 从而可以让开发者采取动作。比如在Meta中，成功被处理，即可更新偏移量，当失败时，重复发送数据。
因此，通过Ack机制，很容易做到保证所有数据均被处理，一条都不漏。<br>
另外需要注意的，当spout触发fail动作时，不会自动重发失败的tuple，需要spout自己重新获取数据，手动重新再发送一次

#### 如何使用ACK机制
spout 在发送数据的时候带上msgid<br>
设置acker数至少大于0；Config.setNumAckers(conf, ackerParal);
<br>
在bolt中完成处理tuple时，执行OutputCollector.ack(tuple), 当失败处理时，执行OutputCollector.fail(tuple); 
<br>
推荐使用IBasicBolt或BaseBasicBolt， 因为IBasicBolt 自动封装了OutputCollector.ack(tuple), 处理失败时，抛出FailedException，则自动执行OutputCollector.fail(tuple)

   
### 日志告警系统logMonitor业务分析

采用架构:flume采集数据,kafka,storm,mysql<br>

#### 业务流程

1.flume的拦截器，然后收集我们的日志数据，然后将我们日志数据，添加一个标识，推送到kafka里面去<br>
2.通过storm与kafka的整合,消费kafka中的数据<br>
3.crontabBolt定时更新数据库当中的规则到map中存储<br>
   业务考虑为:mysql数据库中的各种规则,有可能一直在变,需要定时进行同步.<br>
4.processBolt,数据解析的bolt,主要就是查看发送过来的数据,有没有匹配上我们的规则,匹配上了就往下游发送,匹配不上就不做处理<br>
5.notifyBolt,主要就是给对应的人发送通知,达到日志告警的目的<br>
    这里选用发送邮件的模板进行开发.将发送人的邮箱填写到properties配置文件中,将收件人的信息写入到mysql中,通过storm与mysql的整合,进行查询收件人信息.<br>
6.saveDbBolt,将我们处理的关心的数据保存到数据当中去,便于后续查看.<br>

