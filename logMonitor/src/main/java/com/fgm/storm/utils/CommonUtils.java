package com.fgm.storm.utils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fgm.storm.domain.LogMonitorApp;
import com.fgm.storm.domain.LogMonitorRule;
import com.fgm.storm.domain.LogMonitorRuleRecord;
import com.fgm.storm.domain.LogMonitorUser;
import com.fgm.storm.utils.mail.MailInfo;
import com.fgm.storm.utils.mail.MessageSenderUtil;
import org.apache.commons.lang.StringUtils;


public class CommonUtils implements Serializable{


    private static  StringBuffer buffer = new StringBuffer();


	//定期从数据库中获取我们的规则，然后存储到这个map里面来
	//  appID    Set<log_monitor_rule>
	private static ConcurrentHashMap<Integer,Set<String>> monitorRule  = new ConcurrentHashMap<Integer,Set<String>>();
	
	//获取我们log_monitor_app当中的所有数据一次性全给搂出来，存储到set集合里面去
	private static ConcurrentHashMap<Integer,LogMonitorApp> monitorAppMap = new ConcurrentHashMap<Integer,LogMonitorApp>();
	
	
	//获取appId所有对应的用户，存储到一个Map当中去，需要的时候随时可以获取
	private static ConcurrentHashMap<Integer,Set<String>> monitorUser  = new ConcurrentHashMap<Integer,Set<String>>();
	
	
	//通过json来进行我们的对象与字符串之间的转换
	//private static JSONObject monitorRuleJson = new JSONObject();
	
	//通过该json对象，将我们的log_monitor_rule对象转换成json，将我们的json字符串转换成log_monitor_rule对象
	//private static JSONObject logMonitorRuleJson = new JSONObject();
	
	
	//通过该json对象。将我们的log_monitor_user对象转换成json，或者是将我们的json字符串转换成log_monitor_user对象
	//private static JSONObject logMonitorUserJson = new JSONObject();
	
	

	
	/**
	 * 从数据库当中查询出所有appId对应的规则出来
	 * 每分钟执行一次的定时查询，从数据库中捞出我们的匹配规则，存入到map当中去，供我们每条数据，随时进行匹配规则
	 * 最终将查询出来的数据组织成一个map，map的key就是我们的appId，map的value就是一个set集合，set集合里面装的是
	 * 我们log_monitor_rule对象转换之后的json格式的字符串。
	 * 
	 */
	
	JdbcUtils utils = new JdbcUtils();

	/**
	 * 这个方法，就可以将我们数据库当中的所有的log_monitor_rule当中的数据都同步弄到map当中来存储
	 */
	public synchronized void monitorRule(){
        //将我们数据库当中 log_monitor_rule 这个表当中的所有的数据全部查询出来，
		List<LogMonitorRule> query =utils.queryAllRules();
		for (LogMonitorRule logMonitorRule : query) {

				Integer appId = logMonitorRule.getAppId();
				//判断如果我们的map当中已经包含了这个appId的值
			if(monitorRule.containsKey(appId)){
				Set<String> set = monitorRule.get(appId);
				String jsonString = JSON.toJSONString(logMonitorRule);
				set.add(jsonString);
				monitorRule.put(appId, set);
			}else{
				String ruleJson = JSON.toJSONString(logMonitorRule);
				Set<String> set = new HashSet<String>();
				set.add(ruleJson);
				monitorRule.put(appId, set);
			}
		}
		
		
	}
	
	
	
	
	/**
	 * 定时查询log_monitor_app当中的所有数据，全部加在到jvm当中来随时准备调用
	 */
	
	public synchronized  void monitorApp(){
		List<LogMonitorApp> queryAllApp = utils.queryAllApp();
		for (LogMonitorApp logMonitorApp : queryAllApp) {
			monitorAppMap.put(logMonitorApp.getAppId(), logMonitorApp);
		}
	}
	
	/**
	 * 定时查询 log_monitor_user表中的所有数据，形成一个map结构的数据，map的key就是我们的appId，map的value就是我们的set集合，
	 * set集合里面装的是我们的log_monitor_user对象的json格式的字符串？？这里为啥要装json格式的字符串，不直接装对象更方便？？？
	 */

	/**
	 * 同步我们的appId对应的通知的人是哪些，全部装到一个map当中，准备后续调用
	 *  appId   set<通知人></>
	 */
	
	public synchronized  void monitorUser(){
			List<LogMonitorUser> queryAllUser = utils.queryAllUser();
			for (LogMonitorUser logMonitorUser : queryAllUser) {
				String jsonString = JSON.toJSONString(logMonitorUser);
				if(monitorUser.containsKey(logMonitorUser.getChargeAppId())){
					Set<String> set = monitorUser.get(logMonitorUser.getChargeAppId());
					set.add(jsonString);
					monitorUser.put(logMonitorUser.getChargeAppId(), set);
				}else{
					Set<String> userSet = new HashSet<String>();
					userSet.add(jsonString);
					monitorUser.put(logMonitorUser.getChargeAppId(), userSet);
				}
			}
		}
	
	
	
	
	/**
	 * 返回LogMonitorRule 这个对象的json格式字符串
	 * @param appId
	 * @param datas  我们发送过来的一长串数据  exception  xx   cxx.service
	 * @return
	 */
	public static  String checkRules(String appId,String datas) {
		Set<String> set = monitorRule.get(Integer.parseInt(appId));
		String rule = "";
		if(null != set && set.size() > 0){
			for (String logMonitorRule : set) {
				LogMonitorRule logMonitor = JSON.parseObject(logMonitorRule, LogMonitorRule.class);
				if(datas.contains(logMonitor.getKeyword())){
					//匹配上了关键词，返回匹配的规则
					rule = JSON.toJSONString(logMonitorRule);
					break;
				}
			}
		}
		
		return rule;
	}



    public static void notifyPeople(String rule,String errorLog){
        if(StringUtils.isNotEmpty(rule)){
            //  String rule = input.getStringByField("rule");
            rule = rule.replace("\\", "");
            rule = rule.substring(1, rule.length()-1);
            System.out.println(rule);
            //  String line = input.getStringByField("errorLog");
            //发短信，发邮件，通知相关人员
            String[] split = errorLog.split("\001");
            String appId = split[0];
            String datas = split[1];
            //通过appId查询出该APP对应的负责人
            //"#appname#=hello&#rid#=1&#keyword#=exception";
            LogMonitorRule parseObject = JSON.parseObject(rule, LogMonitorRule.class);
            LogMonitorApp logMonitorApp = CommonUtils.getMonitorAppMap().get(Integer.parseInt(appId));
            //组织我们的邮件发送的内容，拼接字符串
            String sendMsg = "#appname#="+logMonitorApp.getName()+"&#rid#="+parseObject.getRuleId()+"&#keyword#="+parseObject.getKeyword();

            //获取这个appId所有需要通知的人
            Set<String> set = CommonUtils.getAppIdToNotifyUser().get(Integer.parseInt(appId));
            List<String> mailList = new ArrayList<String>();
            //发送短信
            for (String string : set){
                LogMonitorUser logMonitorUser = JSON.parseObject(string, LogMonitorUser.class);
                //		ShortMessageUtil.sendShortMessage(logMonitorUser.getMobile(), sendMsg);
                mailList.add(logMonitorUser.getEmail());
            }
            //发送邮件
            MailInfo info = new MailInfo("日志监控告警系统","尊敬的项目负责人您好，你负责的项目出现了bug，请及时查看并解决",mailList,mailList);
            MessageSenderUtil.sendMail(info);
        }

    }


    public static void insertToDb(String rule,String errorLog){
        System.out.println(rule.replace("\\", ""));
        System.out.println(errorLog);
        rule = rule.replace("\\", "");
        rule = rule.substring(1, rule.length()-1);
        LogMonitorRule logMonitorRule = JSON.parseObject(rule,LogMonitorRule.class);
        String[] split = errorLog.split("\001");
        LogMonitorRuleRecord record = new LogMonitorRuleRecord();
        record.setAppId(Integer.parseInt(split[0]));
        record.setIsClose(0);
        record.setIsEmail(1);
        record.setIsPhone(1);
        record.setNoticeInfo(split[1]);
        record.setRuleId(logMonitorRule.getRuleId());
        record.setCreateDate(new Date());
        record.setUpdateDate(new Date());
        JdbcUtils.saveRuleRecord(record);
    }


	public static Map<Integer, Set<String>> getMap() {
		return monitorRule;
	}




	public static void setMap(ConcurrentHashMap<Integer, Set<String>> map) {
        CommonUtils.monitorRule = map;
	}




	public static Map<Integer, LogMonitorApp> getMonitorAppMap() {
		return monitorAppMap;
	}




	public static void setMonitorAppMap(ConcurrentHashMap<Integer, LogMonitorApp> monitorAppMap) {
		CommonUtils.monitorAppMap = monitorAppMap;
	}




	public static Map<Integer, Set<String>> getAppIdToNotifyUser() {
		return monitorUser;
	}




	public static void setAppIdToNotifyUser(ConcurrentHashMap<Integer, Set<String>> appIdToNotifyUser) {
		CommonUtils.monitorUser = appIdToNotifyUser;
	}



}
