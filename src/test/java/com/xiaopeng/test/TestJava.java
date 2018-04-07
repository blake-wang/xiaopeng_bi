package com.xiaopeng.test;

import java.util.Map;
import java.util.Set;

/**
 * Created by bigdata on 18-3-19.
 */
public class TestJava {
    public static void main(String[] args) {
//        Map<String, String> getenv = System.getenv();
//        Set<Map.Entry<String, String>> entries = getenv.entrySet();
//        for (Map.Entry<String, String> entry : entries) {
//            System.out.println(entry.getKey() + "---" + entry.getValue());
//        }

//        String username = System.getenv("USERNAME");
//        System.out.println(Integer.valueOf(username));

        String property = System.getProperty("user.home");
        System.out.println(property);
        String s = System.setProperty("user.name", "/home/dadadad");
    }
}
