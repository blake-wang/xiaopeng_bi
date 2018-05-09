package com.xiaopeng.bi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 * hive客户端使用：
 * 判断一个user_id是否连续登录k天
 */
public class isExistUDF extends UDF {
    public int evaluate(List input,int k) {

        return 0;
    }
}
