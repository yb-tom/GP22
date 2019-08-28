package com.utils;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Description: 数据库连接池类
 * @author dinglq
 */
public class ConnectPool {
    private static Logger log = Logger.getLogger(ConnectPool.class);
    private static BasicDataSource bs = null;

    /**
     * 创建数据源
     * @return
     */
    public static BasicDataSource getDataSource() throws Exception{
        if(bs==null){
            bs = new BasicDataSource();
            bs.setDriverClassName("com.mysql.jdbc.Driver");
            bs.setUrl("jdbc:mysql://hadoop101:3306/test?useUnicode=true&characterEncoding=utf8");
            bs.setUsername("root");
            bs.setPassword("123456");
            bs.setMaxActive(200);//设置最大并发数
            bs.setInitialSize(30);//数据库初始化时，创建的连接个数
            bs.setMinIdle(50);//最小空闲连接数
            bs.setMaxIdle(200);//数据库最大连接数
            bs.setMaxWait(1000);
            bs.setMinEvictableIdleTimeMillis(60*1000);//空闲连接60秒中后释放
            bs.setTimeBetweenEvictionRunsMillis(5*60*1000);//5分钟检测一次是否有死掉的线程
            bs.setTestOnBorrow(true);
        }
        return bs;
    }

    /**
     * 释放数据源
     */
    public static void shutDownDataSource() throws Exception{
        if(bs!=null){
            bs.close();
        }
    }

    /**
     * 获取数据库连接
     * @return
     */
    public static Connection getConnection(){
        Connection con=null;
        try {
            if(bs!=null){
                con=bs.getConnection();
            }else{
                con=getDataSource().getConnection();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return con;
    }

    /**
     * 关闭连接
     */
    public static void closeCon(ResultSet rs, PreparedStatement ps, Connection con){
        if(rs!=null){
            try {
                rs.close();
            } catch (Exception e) {
                log.error("关闭结果集ResultSet异常！"+e.getMessage(), e);
            }
        }
        if(ps!=null){
            try {
                ps.close();
            } catch (Exception e) {
                log.error("预编译SQL语句对象PreparedStatement关闭异常！"+e.getMessage(), e);
            }
        }
        if(con!=null){
            try {
                con.close();
            } catch (Exception e) {
                log.error("关闭连接对象Connection异常！"+e.getMessage(), e);
            }
        }
    }
}

