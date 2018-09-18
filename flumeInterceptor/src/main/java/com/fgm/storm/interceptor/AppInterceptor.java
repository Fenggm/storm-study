package com.fgm.storm.interceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class AppInterceptor implements Interceptor {

    private String appId;

    public AppInterceptor(String appId){
        this.appId = appId;
    }

    @Override
    public Event intercept(Event event){
        String message = null;
        try {
            message = new String(event.getBody(), "utf-8");
        } catch (UnsupportedEncodingException e) {
            message = new String(event.getBody());
        }

        if (StringUtils.isNotBlank(message)) {
            message =  this.appId + "\001" + message;
            event.setBody(message.getBytes());
            return event;
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list){
        List resultList = new ArrayList();
        for (Event event : list) {
            Event r = intercept(event);
            if (r != null) {
                resultList.add(r);
            }
        }
        return resultList;
    }

    @Override
    public void close()
    {
    }

    @Override
    public void initialize(){

    }

    public static class AppInterceptorBuilder implements Builder{

        private String appId;

        @Override
        public Interceptor build() {
            return new AppInterceptor(this.appId);
        }

        @Override
        public void configure(Context context)
        {
            this.appId = context.getString("appId", "default");
            System.out.println("appId:" + this.appId);
        }
    }


}
