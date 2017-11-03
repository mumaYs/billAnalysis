package com.ccfsoft.bigdata.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Java时间工具类
 */
public class TimeUtil {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 获取当前日期时间
     * @return
     */
    public static final String getLocalDataTime(){
        LocalDateTime dateTime = LocalDateTime.now();
        return dateTime.format(formatter);
    }

    public static void main(String[] args) {
    }
}
