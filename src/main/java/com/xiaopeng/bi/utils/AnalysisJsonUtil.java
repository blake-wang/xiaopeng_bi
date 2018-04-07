package com.xiaopeng.bi.utils;

import com.google.gson.Gson;
import com.xiaopeng.bi.bean.AndroidErrorBean;
import com.xiaopeng.bi.bean.GameInsideRoleData;
import com.xiaopeng.bi.bean.MoneyMasterBean;

import java.io.Serializable;

/**
 * Created by kequan on 3/6/17.
 */
public class AnalysisJsonUtil implements Serializable {
    public static Gson gson = new Gson();

    public static AndroidErrorBean DecodingAndAnalysisAndroidError(String error) {
        try {
            return gson.fromJson(DecodindlogUtil.serviceDecode(error), AndroidErrorBean.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static GameInsideRoleData AnalysisGameInsideData(String data) {
        try {
            return gson.fromJson(data, GameInsideRoleData.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static MoneyMasterBean AnalysisMoneyMasterData(String data) {
        try {
            return gson.fromJson(data, MoneyMasterBean.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
