package com.fgm.storm.utils.mail;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * 功能描述：
 * <p/>
 * <p/>
 * ----------------------------
 * 姓名：fgm
 */

public class MailCenterConstant implements Serializable {


    public static final long serialVersionUID = 472077651977893328L;
    public final static String PROTOCOL = "smtp";
    // 设置发件人使用的SMTP服务器、用户名、密码
    public static String SMTP_SERVER;
    public static String FROM_ADDRESS;
    public static String USER;
    public static String PWD;


    public MailCenterConstant(){

    }

     static {
        Properties props = new Properties();
        InputStream in = Object.class.getResourceAsStream("/mail.properties");
        try {
            props.load(in);
            SMTP_SERVER = props.getProperty("MAIL_SMTP_SERVER").trim();
            FROM_ADDRESS = props.getProperty("MAIL_FROM_ADDRESS").trim();
            USER = props.getProperty("MAIL_USER").trim();
            PWD = props.getProperty("MAIL_PWD").trim();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static String getSmtpServer() {
        return SMTP_SERVER;
    }

    public static String getFromAddress() {
        return FROM_ADDRESS;
    }

    public static String getUSER() {
        return USER;
    }

    public static String getPWD() {
        return PWD;
    }
}
