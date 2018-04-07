package com.xiaopeng.bi.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by denglh on 2016/10/26.
 * 目的：从mysql中定时查询数据，然后推送到redis中，发行相关维度数据到redis
 */
public class publicFxGgameTbPush2Redis {

    public static void publicGgameTbPush2Redis() throws SQLException, ClassNotFoundException {

        try {
            Connection conn = JdbcUtil.getXiaopeng2FXConn();
            Statement stmt = conn.createStatement();

            JedisPool pool = JedisUtil.getJedisPool();
            Jedis jedis = pool.getResource();

            fx2Dim(stmt, jedis);
            fx2DimGame(stmt, jedis);

            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    /**
     * 推送主游戏和组
     *
     * @param stmt
     * @param jedis
     * @throws SQLException
     */
    private static void fx2DimGame(Statement stmt, Jedis jedis) throws SQLException {
        //发行主游戏数据加载到后面
        String sqlM = "select distinct old_game_id as id,game_id as mainid,sdk.system_type,base.game_name main_name,sdk.group_id publish_group_id from game_sdk sdk join game_base base on base.id=sdk.game_id";
        ResultSet rsM = stmt.executeQuery(sqlM);
        while (rsM.next()) {
            Map<String, String> game_main = new HashMap<String, String>();
            game_main.put("mainid", rsM.getString("mainid") == null ? "" : rsM.getString("mainid"));
            game_main.put("system_type", rsM.getString("system_type") == null ? "" : rsM.getString("system_type"));
            game_main.put("main_name", rsM.getString("main_name") == null ? "" : rsM.getString("main_name"));
            game_main.put("publish_group_id", rsM.getString("publish_group_id") == null ? "" : rsM.getString("publish_group_id"));
            jedis.hmset(rsM.getString("id") + "_publish_game", game_main);
        }
    }

    /**
     * 发行维度数据，推广账号 负责人 推广渠道 等等
     *
     * @param stmt
     * @param jedis
     * @throws SQLException 负责人：渠道负责人 或则 媒介账号负责人（不同时存在）
     */
    private static void fx2Dim(Statement stmt, Jedis jedis) throws SQLException {

        //媒介账号 merchant.medium_package
        String sqla = "select subpackage_id as pkg_code,a.merchant_id,merchant from medium_package a join merchant b on a.merchant_id=b.merchant_id ";
        ResultSet rs = stmt.executeQuery(sqla);
        while (rs.next()) {
            Map<String, String> merchant = new HashMap<String, String>();  //hashmap
            merchant.put("pkg_code", rs.getString("pkg_code") == null ? "0" : rs.getString("pkg_code"));
            merchant.put("medium_account", rs.getString("merchant") == null ? "0" : rs.getString("merchant"));
            jedis.hmset(rs.getString("pkg_code") + "_pkgcode", merchant); //加载到redis
        }

        //推广渠道1 channel_main.main_name
        String sqlc1 = "select distinct pkg_code,main_name promotion_channel from channel_pkg a join channel_main b on b.id=a.main_id ";
        rs = stmt.executeQuery(sqlc1);
        while (rs.next()) {
            Map<String, String> tgqd1 = new HashMap<String, String>();
            tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
            tgqd1.put("promotion_channel", rs.getString("promotion_channel") == null ? "" : rs.getString("promotion_channel"));
            jedis.hmset(rs.getString("pkg_code") + "_pkgcode", tgqd1); //加载到redis
        }

        //推广渠道2  agent.agent_name
        String sqlc2 = "select subpackage_id pkg_code,agent_name promotion_channel from medium_package a \n" +
                "join merchant b on a.merchant_id=b.merchant_id join agent c on c.id=b.agent_id  ";
        rs = stmt.executeQuery(sqlc2);
        while (rs.next()) {
            Map<String, String> tgqd1 = new HashMap<String, String>();
            tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
            tgqd1.put("promotion_channel", rs.getString("promotion_channel") == null ? "" : rs.getString("promotion_channel"));
            jedis.hmset(rs.getString("pkg_code") + "_pkgcode", tgqd1); //加载到redis
        }

        //渠道负责人
        String sqlms = "select pkg_code,us.name head_people  from channel_pkg c join user us on us.id=c.manager ";
        rs = stmt.executeQuery(sqlms);
        while (rs.next()) {
            Map<String, String> tgqd1 = new HashMap<String, String>();
            tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
            tgqd1.put("head_people", rs.getString("head_people") == null ? "" : rs.getString("head_people"));
            for (int ii = -1; ii <= 2; ii++) {
                String dt = DateUtils.getday(ii);
                jedis.hmset(rs.getString("pkg_code") + "_" + dt + "_pkgcode", tgqd1); //加载到redis
                jedis.expire(rs.getString("pkg_code") + "_" + dt + "_pkgcode", 3600 * 24 * 3);
            }

        }

        //媒介账号负责人
        String sqlms1 = "select subpackage_id as pkg_code,us.name head_people  from medium_package cl join user us on us.id=cl.user_id  ";
        rs = stmt.executeQuery(sqlms1);
        while (rs.next()) {
            Map<String, String> tgqd1 = new HashMap<String, String>();
            tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
            tgqd1.put("head_people", rs.getString("head_people") == null ? "" : rs.getString("head_people"));
            for (int ii = -1; ii <= 2; ii++) {
                String dt = DateUtils.getday(ii);
                jedis.hmset(rs.getString("pkg_code") + "_" + dt + "_pkgcode", tgqd1); //加载到redis
                jedis.expire(rs.getString("pkg_code") + "_" + dt + "_pkgcode", 3600 * 24 * 3);
            }
        }


        //推广模式
        for (int ii = -1; ii <= 1; ii++) {
            String dt = DateUtils.getday(ii);
            String sqlms2 = "select pkg_code,'" + dt + "' as dt,b.promotion promotion_mode from channel_pkg a join channel_pkg_conf b on a.id=b.pkg_id\n" +
                    "where left(b.start_time,10)<='" + dt + "' and left(b.end_time,10)>='" + dt + "'";
            rs = stmt.executeQuery(sqlms2);
            while (rs.next()) {
                Map<String, String> tgqd1 = new HashMap<String, String>();
                tgqd1.put("pkg_code", rs.getString("pkg_code") == null ? "" : rs.getString("pkg_code"));
                tgqd1.put("promotion_mode", rs.getString("promotion_mode") == null ? "" : rs.getString("promotion_mode"));
                jedis.hmset(rs.getString("pkg_code") + "_" + dt + "_pkgcode", tgqd1); //加载到redis
                jedis.expire(rs.getString("pkg_code") + "_" + dt + "_pkgcode", 3600 * 24 * 3);
            }
        }
    }

}
