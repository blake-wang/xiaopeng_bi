package com.xiaopeng.bi.bean;

/**
 * Created by bigdata on 17-11-6.
 */
public class RegiDataBean {
//    //game_account  game_id  expand_channel._3 reg_time  imei,os
//      (arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14), arr(11).toLowerCase)
    private String game_account ;
    private int game_id;
    private String expand_channel;
    private String reg_time;
    private String imei;
    private String os;

    public RegiDataBean(String game_account, int game_id, String expand_channel, String reg_time, String imei, String os) {
        this.game_account = game_account;
        this.game_id = game_id;
        this.expand_channel = expand_channel;
        this.reg_time = reg_time;
        this.imei = imei;
        this.os = os;
    }

    public String getGame_account() {
        return game_account;
    }

    public void setGame_account(String game_account) {
        this.game_account = game_account;
    }

    public int getGame_id() {
        return game_id;
    }

    public void setGame_id(int game_id) {
        this.game_id = game_id;
    }

    public String getExpand_channel() {
        return expand_channel;
    }

    public void setExpand_channel(String expand_channel) {
        this.expand_channel = expand_channel;
    }

    public String getReg_time() {
        return reg_time;
    }

    public void setReg_time(String reg_time) {
        this.reg_time = reg_time;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }
}
