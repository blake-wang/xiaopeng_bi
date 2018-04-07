package com.xiaopeng.bi.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by denglh on 2016/11/1.
 */
public class bi_opera_data_export {
    private static BufferedWriter writer;

    public static void main(String args[]) throws SQLException, IOException {
        //date
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        String yesterday = new SimpleDateFormat("yyyy-MM-dd ").format(cal.getTime());
        //heard
        String header = "日期,游戏,渠道,新增用户,活跃用户,付费用户,新增付费数,付费金额,新增用户付费率,活跃用户付费率,平均每用户付费金额（arpu),人均付费金额,1日后留存,2日后留存,3日后留存,4日后留存,5日后留存,6日后留存,14日后留存,29日后留存";
        File file = new File("/home/hduser/crontabFiles/opensdk_" + yesterday.trim().replace("-", "") + ".csv");
        file.delete();
        try {
            writer = new BufferedWriter(new FileWriter(file));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //connect xiaopeng2
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        String game_keys = "";
        String sql = "select bc.ch_game_id as game_key,bg.name from bgame bg join bgame_channel bc on bc.game_id=bg.id where bg.status=0 and bc.status=0 and bc.channel_id in(22,23)";
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        ResultSet rs = stmt.executeQuery(sql);
        Map<String, String> bgame = new HashMap<String, String>();

        while (rs.next()) {
            game_keys = game_keys + "','" + rs.getString("game_key");
            bgame.put(rs.getString("game_key") == null ? "" : rs.getString("game_key"), rs.getString("name") == null ? "" : rs.getString("name"));
        }
        game_keys = game_keys.substring(2, game_keys.length()) + "'";

        //bi connection
        Connection connBi = JdbcUtil.getConn();
        String sqlBi = "select statistics_date, game_key,channel_id,add_users,active_users,pay_users,add_pay_users,pay_amount,add_pay_rate,active_pay_rate,pay_active_per,pay_amount_per,retained_1day,retained_2day,retained_3day," +
                "retained_4day,retained_5day,retained_6day,retained_14day,retained_29day from bi_opera_data where game_key in(" + game_keys + ") and statistics_date=?";
        System.out.println(sqlBi);
        PreparedStatement psBi = connBi.prepareStatement(sqlBi);
        psBi.setString(1, yesterday);
        ResultSet rsBi = psBi.executeQuery();
        //table header
        writer.append(header);
        writer.newLine();
        while (rsBi.next()) {
            String name = bgame.get(rsBi.getString("game_key"));
            System.out.println(name);
            try {
                writer.append(rsBi.getString("statistics_date") + "," +
                        bgame.get(rsBi.getString("game_key")) + "," +
                        "疯趣" + "," +
                        rsBi.getString("add_users") + "," +
                        rsBi.getString("active_users") + "," +
                        rsBi.getString("pay_users") + "," +
                        rsBi.getString("add_pay_users") + "," +
                        rsBi.getString("pay_amount") + "," +
                        rsBi.getString("add_pay_rate") + "," +
                        rsBi.getString("active_pay_rate") + "," +
                        rsBi.getString("pay_active_per") + "," +
                        rsBi.getString("pay_amount_per") + "," +
                        rsBi.getString("retained_1day") + "," +
                        rsBi.getString("retained_2day") + "," +
                        rsBi.getString("retained_3day") + "," +
                        rsBi.getString("retained_4day") + "," +
                        rsBi.getString("retained_5day") + "," +
                        rsBi.getString("retained_6day") + "," +
                        rsBi.getString("retained_14day") + "," +
                        rsBi.getString("retained_29day")
                );
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != writer) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


}

