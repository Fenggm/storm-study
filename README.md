###storm-hdfs为storm与hdfs的结合使用.
这里主要调用官方的API进行测试.  http://storm.apache.org/releases/1.1.2/storm-hdfs.html
<br>
其中,com.fgm.storm.stormack中为ack机制的测试案例.<br>
通过Ack机制，spout发送出去的每一条消息，都可以确定是被成功处理或失败处理， 从而可以让开发者采取动作。比如在Meta中，成功被处理，即可更新偏移量，当失败时，重复发送数据。
因此，通过Ack机制，很容易做到保证所有数据均被处理，一条都不漏。<br>
另外需要注意的，当spout触发fail动作时，不会自动重发失败的tuple，需要spout自己重新获取数据，手动重新再发送一次

####如何使用ACK机制
spout 在发送数据的时候带上msgid<br>
设置acker数至少大于0；Config.setNumAckers(conf, ackerParal);
<br>
在bolt中完成处理tuple时，执行OutputCollector.ack(tuple), 当失败处理时，执行OutputCollector.fail(tuple); 
<br>
推荐使用IBasicBolt或BaseBasicBolt， 因为IBasicBolt 自动封装了OutputCollector.ack(tuple), 处理失败时，抛出FailedException，则自动执行OutputCollector.fail(tuple)

