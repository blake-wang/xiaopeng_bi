package com.xiaopeng.bi.udf;

import org.apache.spark.sql.api.java.UDF2;

import java.util.*;

/**
 * Created by kequan on 4/10/17.
 * 根据
 * s 分层模式
 * s2 支付金额
 * 获取分层比例
 */
public class GameDivideLadderUDF implements UDF2<String, String, String> {
    public String call(String s, String s2) throws Exception {
        if (s != null) {
            String[] ladder = s.split("\\|");
            Map<Integer, String> ladders = new HashMap<Integer, String>();
            for (int i = 0; i < ladder.length; i++) {
                if (ladder[i].contains("=") && ladder[i].split("=").length == 2) {
                    ladders.put(new Integer(ladder[i].split("=")[0]), ladder[i].split("=")[1]);
                }
            }
            List<Map.Entry<Integer, String>> infoIds = new ArrayList<Map.Entry<Integer, String>>(ladders.entrySet());
            Collections.sort(infoIds, new Comparator<Map.Entry<Integer, String>>() {
                public int compare(Map.Entry<Integer, String> o1, Map.Entry<Integer, String> o2) {
                    return o1.getKey() - o2.getKey();
                }
            });
            int index = 0;
            for (int i = 0; i < infoIds.size(); i++) {
                if (s2 == null) {
                    index = 0;
                } else if (new Integer(s2) >= new Integer(infoIds.get(i).toString().split("=")[0])) {
                    index = i;
                }
            }
            return infoIds.get(index).toString().split("=")[1];
        } else {
            // CONCAT("0","=",game_divide.my_rate,game_divide_ladder_rz_cache.ladder_price_ratios)
            // 如果 game_divide.my_rate=null  那么 "0","=",game_divide.my_rate 都为空 代码执行结果
            return "0";
        }


    }


}
