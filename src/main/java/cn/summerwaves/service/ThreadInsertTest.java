package cn.summerwaves.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by summerwaves on 2017/7/16.
 */
public class ThreadInsertTest extends Thread {

    public static void main(String[] args) throws Exception {

        for (int i=0;i<10;i++) {   //使用i个线程插入数据
            new ThreadInsertTest().start();
        }
        long start = System.currentTimeMillis();


        long end = System.currentTimeMillis();
        System.out.println("插入1000数据总耗时"+(end - start)+"ms");

    }

    public void run() {
        Connection conn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO user (user_name,password,sex) VALUE ('username','password',1)";

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/user?characterEncoding=utf8&useSSL=true&useServerPrepStmts=false&rewriteBatchedStatements=true", "root", "zqd19931007");
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(sql);
            long start = System.currentTimeMillis();

            for (int i =0; i<100;i++) {
                ps.addBatch(sql);
                if (i % 1000 == 0) {
                    ps.executeBatch(); //每10000条数据提交一次，以防内存溢出
                    conn.commit();
                }
            }
            ps.executeBatch(); //最后再提交一次，若插入数据不是整数也没有关系
            conn.commit();
            long end = System.currentTimeMillis();
            System.out.println("插入100条数据所需时间" + (end - start)+"ms");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }  finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
