package com.xiaopeng.bi.checkdata;


import com.xiaopeng.bi.utils.Commons;
import com.xiaopeng.bi.utils.JdbcUtil;
import com.xiaopeng.bi.utils.JedisUtil;
import com.xiaopeng.bi.utils.dao.SubCenturionDao;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by denglh on 2016/10/26.
 * 目的：由于redis中可能出现账号或者通行证等信息的遗漏，避免出现问题，半个小时监测一次
 */
public  class MissInfo2Redis {

    /**
     * 对丢失的下级账号数据进行补充
     * @param account
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void checkAccountExists(String account,String loginTime) throws SQLException, ClassNotFoundException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        Connection connbi = JdbcUtil.getConn();
        JedisPool pool= JedisUtil.getJedisPool();
        Jedis jedis= pool.getResource();

        String gameAccount="";
        String gamiId="0";
        String regiTime="";
        String regiDate="";
        String imei="";
        String ip="23.4.12.4";
        String promoCode="";
        String userCode="";
        String memberId="";
        String userAccount="";
        String pkgCode="";
        int groupId=0;
        String bindMember="";
        String memberRegiTime="";
        String sql = "\n" +
                "select spa.account,subordinate_code,bgm.addtime,bgm.imie,bgm.gameid,bgm.uid,puer.`code`,puer.username,puer.group_id,puer.create_time,mm.username bindMember,\n" +
                "bgm.channel_owner channel_id,bgm.os as reg_os_type,bind_member_id\n" +
                " from specialsrv_account spa join bgameaccount bgm on bgm.account=spa.account\n" +
                "join promo_user puer on puer.member_id=bgm.uid\n" +
                "left join member mm on mm.id=bgm.bind_member_id\n" +
                "where spa.account='accountList'".replace("accountList",account);
        System.out.println(sql);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            gameAccount=rs.getString("account");
            gamiId=rs.getString("gameid");
            regiTime=rs.getString("addtime");
            regiDate=rs.getString("addtime").substring(0,10);
            userCode=rs.getString("subordinate_code");
            imei= Commons.getImei(rs.getString("imie"));
            promoCode=rs.getString("code");
            memberId=rs.getString("uid");
            groupId=rs.getInt("group_id");
            memberRegiTime=rs.getString("create_time");
            bindMember=rs.getString("bindMember");
            pkgCode=promoCode+"~"+userCode;
            userAccount=rs.getString("username");
            SubCenturionDao.regiInfoProcessAccountBs(gameAccount,gamiId,regiTime,regiDate,imei,ip,promoCode,userCode,pkgCode,memberId,userAccount,groupId,bindMember,memberRegiTime,loginTime,connbi);
            SubCenturionDao.regiInfoProcessPkgStat(regiDate,promoCode,userCode,pkgCode,memberId,userAccount,gamiId,groupId,memberRegiTime,1,1,connbi);

            Map<String,String> accountredis =  new HashMap<String,String>();
            accountredis.put("userid", memberId);
            accountredis.put("game_account", gameAccount.trim().toLowerCase());
            accountredis.put("game_id",gamiId);
            accountredis.put("reg_time",regiTime);
            accountredis.put("reg_resource", "8");
            accountredis.put("channel_id",rs.getString("channel_id")==null?"0":rs.getString("channel_id"));
            accountredis.put("owner_id",memberId);
            accountredis.put("bind_member_id", rs.getString("bind_member_id")==null?"0":rs.getString("bind_member_id"));
            accountredis.put("status","1");
            accountredis.put("reg_os_type", rs.getString("reg_os_type")==null?"UNKNOW":rs.getString("reg_os_type"));
            accountredis.put("expand_code",promoCode);
            accountredis.put("expand_code_child",userCode);
            if(gameAccount!=null) {
                jedis.hmset(gameAccount.trim().toLowerCase(), accountredis);
            }

        }
        stmt.close();
        conn.close();
        connbi.close();
        pool.returnResource(jedis);
        pool.close();
    }

    /**
     *
     * @param memberList
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 监测redis中是否存在程序找不到的会员，若有则取出从库中补全
     */
    public static void checkMember(String memberList) throws SQLException, ClassNotFoundException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();

        String sql = "select mm.id,mm.username,mm.tel,mm.qq,sex,`desc` as descr,email,regtype,pu.member_grade grade,vip_note,pu.`code` as promo_code,pu.member_id promo_member_id,pu.`code` invite_code,regtime,'1' as status \n" +
                "from member mm left join promo_user pu  on pu.member_id=mm.id where mm.id in(memberList) ".replace("memberList",memberList);
        System.out.println(sql);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        ResultSet rs = stmt.executeQuery(sql);

        try {
            JedisPool pool= JedisUtil.getJedisPool();
            Jedis jedis=pool.getResource();
            while (rs.next()) {
                //补充到redis
                Map<String,String> member =  new HashMap<String,String>();  //hashmap
                member.put("username",rs.getString("username")==null?"":rs.getString("username"));
                member.put("tel",rs.getString("tel")==null?"":rs.getString("tel"));
                member.put("qq",rs.getString("qq")==null?"":rs.getString("qq"));
                member.put("sex",rs.getString("sex")==null?"0":rs.getString("sex"));
                member.put("descr",rs.getString("descr")==null?"":rs.getString("descr"));
                member.put("email",rs.getString("email")==null?"":rs.getString("email"));
                member.put("regtype",rs.getString("regtype")==null?"0":rs.getString("regtype"));  //注册类型
                member.put("grade",rs.getString("grade")==null?"":rs.getString("grade"));   //通行证等级
                member.put("vip_note",rs.getString("vip_note")==null?"":rs.getString("vip_note"));
                member.put("promo_code",rs.getString("promo_code")==null?"0":rs.getString("promo_code"));  //黑金卡推广码
                member.put("promo_member_id",rs.getString("promo_member_id")==null?"0":rs.getString("promo_member_id"));
                member.put("invite_code",rs.getString("invite_code")==null?"":rs.getString("invite_code"));  //邀请码
                member.put("regtime",rs.getString("regtime")==null?"":rs.getString("regtime")); //注册时间
                member.put("status",rs.getString("status")==null?"1":rs.getString("status"));
                jedis.hmset(rs.getString("id").trim()+"_member", member); //加载到redis
                if(rs.getString("username")!=null)
                    jedis.set(rs.getString("username"),rs.getString("id").trim());
                if(rs.getString("promo_code")!=null)
                    jedis.set(rs.getString("promo_code"),rs.getString("id").trim());
                System.out.println(rs.getString("id").trim()+"_member");
            }
            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (Exception e)
        {
            System.out.println(e);
        }

    }
    /**
     *
     * @param accountList
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 账号补充
     */
    public static int checkAccount(String accountList) throws SQLException, ClassNotFoundException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        int flag=1;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            flag=0;
        }
        //
        String sql = "SELECT 'checkAccount' requestid,'' as userid,a.account as game_account,a.gameid as  game_id,a.addtime as reg_time,'' reg_resource,a.account_channel as  channel_id,\n" +
                " a.uid as owner_id,a.bind_member_id as bind_member_id,a.state status,if(a.os='','UNKNOW',os) as reg_os_type,if(b.code is null,'',b.code) as expand_code,'no_acc' as expand_channel,subordinate_code FROM bgameaccount a \n" +
                " left join promo_user b on a.uid=b.member_id \n" +
                " left join specialsrv_account sa on sa.account=a.account\n" +
                " where a.account in('accountList')".replace("accountList",accountList);
        System.out.println(sql);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            JedisPool pool= JedisUtil.getJedisPool();
            Jedis jedis=pool.getResource();
            while (rs.next()) {
                Map<String,String> account =  new HashMap<String,String>();
                account.put("requestid", rs.getString("requestid")==null?"":rs.getString("requestid"));
                account.put("userid", rs.getString("userid")==null?"":rs.getString("userid"));
                account.put("game_account", rs.getString("game_account")==null?"":rs.getString("game_account").trim().toLowerCase());
                account.put("game_id", rs.getString("game_id")==null?"0":rs.getString("game_id"));
                account.put("reg_time",rs.getString("reg_time")==null?"0000-00-00":rs.getString("reg_time"));
                account.put("reg_resource", rs.getString("reg_resource")==null?"2":rs.getString("reg_resource"));
                account.put("channel_id",rs.getString("channel_id")==null?"0":rs.getString("channel_id"));
                account.put("owner_id",rs.getString("owner_id")==null?"0":rs.getString("owner_id"));
                account.put("bind_member_id", rs.getString("bind_member_id")==null?"0":rs.getString("bind_member_id"));
                account.put("status",rs.getString("status")==null?"1":rs.getString("status"));
                account.put("reg_os_type", rs.getString("reg_os_type")==null?"UNKNOW":rs.getString("reg_os_type"));
                account.put("expand_code",rs.getString("expand_code")==null?"":rs.getString("expand_code"));
                if(rs.getString("game_account")!=null) {
                    jedis.hmset(rs.getString("game_account").trim().toLowerCase(), account);
                }
            }
            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (Exception e)
        {
            System.out.println(e);
            flag=0;
        }
        return flag;
    }

    /**
     *
     * @param accountList
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 黑专服下级补充
     */
    public static int checkAccountSubCen(String accountList) throws SQLException, ClassNotFoundException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        int flag=1;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            flag=0;
        }
        //
        String sql = "SELECT 'checkAccountSubCen' requestid,'' as userid,a.account as game_account,a.gameid as  game_id,a.addtime as reg_time,'' reg_resource,a.account_channel as  channel_id,\n" +
                " a.uid as owner_id,a.bind_member_id as bind_member_id,a.state status,if(a.os='','UNKNOW',os) as reg_os_type,if(b.code is null,0,b.code) as expand_code,'no_acc' as expand_channel,subordinate_code FROM bgameaccount a \n" +
                " left join promo_user b on a.uid=b.member_id \n" +
                " join specialsrv_account sa on sa.account=a.account\n" +
                " where a.account in('accountList')".replace("accountList",accountList);
        System.out.println(sql);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            JedisPool pool= JedisUtil.getJedisPool();
            Jedis jedis=pool.getResource();
            while (rs.next()) {
                Map<String,String> account =  new HashMap<String,String>();
                account.put("requestid", rs.getString("requestid")==null?"":rs.getString("requestid"));
                account.put("userid", rs.getString("userid")==null?"":rs.getString("userid"));
                account.put("game_account", rs.getString("game_account")==null?"":rs.getString("game_account").trim().toLowerCase());
                account.put("game_id", rs.getString("game_id")==null?"0":rs.getString("game_id"));
                account.put("reg_time",rs.getString("reg_time")==null?"0000-00-00":rs.getString("reg_time"));
                account.put("reg_resource", rs.getString("reg_resource")==null?"2":rs.getString("reg_resource"));
                account.put("channel_id",rs.getString("channel_id")==null?"0":rs.getString("channel_id"));
                account.put("owner_id",rs.getString("owner_id")==null?"0":rs.getString("owner_id"));
                account.put("bind_member_id", rs.getString("bind_member_id")==null?"0":rs.getString("bind_member_id"));
                account.put("status",rs.getString("status")==null?"1":rs.getString("status"));
                account.put("reg_os_type", rs.getString("reg_os_type")==null?"UNKNOW":rs.getString("reg_os_type"));
                account.put("expand_code",rs.getString("expand_code")==null?"":rs.getString("expand_code"));
                account.put("expand_code_child",rs.getString("subordinate_code")==null?"":rs.getString("subordinate_code"));
                if(rs.getString("game_account")!=null) {
                    jedis.hmset(rs.getString("game_account").trim().toLowerCase(), account);
                }
            }
            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (Exception e)
        {
            System.out.println(e);
            flag=0;
        }
        return flag;
    }


    /**
     *
     * @param accounts,memberid
     * @throws SQLException
     * @throws ClassNotFoundException
     * @function: 监测redis中是否存在程序找不到的会员，若有则取出从库中补全
     */
    public static int checkAccountBindMember(String accounts,String memberid) throws SQLException, ClassNotFoundException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        int flag=1;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            flag=0;
        }
        //子游戏表数据
        String sql = "SELECT id as bind_member_id FROM member a where a.username in('memberid')".replace("memberid",memberid);
        System.out.println(sql);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            JedisPool pool= JedisUtil.getJedisPool();
            Jedis jedis=pool.getResource();
            while (rs.next()) {
                Map<String,String> account =  new HashMap<String,String>();
                account.put("game_account", accounts.toLowerCase());
                account.put("bind_member_id", rs.getString("bind_member_id")==null?"0":rs.getString("bind_member_id"));
                if(accounts!=null) {
                    jedis.hmset(accounts.trim().toLowerCase(), account);
                }
            }
            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (Exception e)
        {
            System.out.println(e);
            flag=0;
        }
        return flag;
    }


    public  static void checkPromoCode(String promoCode) throws SQLException {
        Connection conn = JdbcUtil.getXiaopeng2Conn();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String sql = "SELECT member_id,code promo_code FROM promo_user a where a.code in('promoCode')".replace("promoCode",promoCode);
        System.out.println(sql);
        ResultSet rs = stmt.executeQuery(sql);
        try {
            JedisPool pool= JedisUtil.getJedisPool();
            Jedis jedis=pool.getResource();
            while (rs.next()) {
                if(rs.getString("promo_code")!=null) {
                    jedis.set(rs.getString("promo_code"),rs.getString("member_id").trim());
                }
            }
            stmt.close();
            conn.close();
            pool.returnResource(jedis);
            pool.destroy();
        } catch (Exception e)
        {
            System.out.println(e);
        }
    }
}
