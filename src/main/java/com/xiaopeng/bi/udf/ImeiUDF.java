package com.xiaopeng.bi.udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by bigdata on 18-3-27.
 * 从日志imei中取出有效的
 */
public class ImeiUDF implements UDF1<String, String> {

    public String call(String device_imei) throws Exception {
        String imei = "";
        if (device_imei.contains("&")) {
            //android设备
            String[] fields = device_imei.split("&", -1);
            if (fields.length == 3) {
                //&&
                //24ee9aff51efd6a3&88:6a:b1:fc:01:f7
                //221132093280616&64110abc12e796d5&f8:2f:48:a1:6d:20
                imei = fields[0];

            }
        } else {
            if (device_imei.length() >= 36) {
                //h5游戏 XMgH5Sdk73a411aa7e249846be2e2ae51513597154727 ,只取前36位
                imei = device_imei.substring(0, 36);
            } else {
                //苹果设备 4CB796F3009A4E6C85276ADA2D395151
                imei = device_imei;
            }
        }
        return imei;
    }
}
