package com.xiaopeng.bi.udf;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by bigdata on 18-3-28.
 * Hive客户端使用UDF  ： MD5 加密
 */
public class HiveMD5UDF extends UDF {
    public String evaluate(String input) {
        String s = md5(input).toUpperCase();
        return s;
    }

    //32位小写加密
    public static String md5(String target) {
        return DigestUtils.md5Hex(target);
    }
}
