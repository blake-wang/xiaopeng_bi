package com.xiaopeng.bi.udf;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by bigdata on 17-9-29.
 * 广告监测，激活匹配点击，imei md5 加密转换
 */
public class MD5UDF implements UDF1<String, String> {

    public String call(String imei) throws Exception {

//        862187030168340&1d749c318da5cc5c&f8:23:b2:2b:a1:80
//        A000006016833D&fd4a647b7f5028be&44:04:44:b3:77:53
//        1A3F237E043B476697961E8894E5A6A8

        String newImei = "";
        String newLineImei = "";
        String imei_md5_upper = "";

        //激活日志中imei如果是带&的，取&前面的，并转成大写
        if (imei.contains("&")) {
            newImei = imei.split("&")[0].toUpperCase();
        } else {
            newImei = imei.toUpperCase();
        }

        //如果是ios的imei，给大写的imei加横杠 ，长度等于15的是android设备，长度等于32的是ios设备
        if (newImei.length() == 32) {
            newLineImei = newImei.substring(0, 8) + "-" + newImei.substring(8, 12) + "-" + newImei.substring(12, 16) + "-" + newImei.substring(16, 20) + "-" + newImei.substring(20, 32);
        }
        //如果是android的imei，md5加密后大写
        if (newImei.length() == 15) {
            newLineImei = newImei;
        }
        //ios返回的是 ： imei大写后，加横杠，再md5，再转大写
        //android返回的是 ： imei大写后，md5，再转大写
        imei_md5_upper = md5(newLineImei).toUpperCase();

        return imei_md5_upper;
    }


    public static String md5(String target) {
        return DigestUtils.md5Hex(target);
    }
}
