package cn.summerwaves.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by summerwaves on 2017/7/16.
 */
public class ThreadInsertTest2 {
    private int nThread;
    private CountDownLatch startGate;
    private CountDownLatch endGate;


    public static void main(String[] args) {
        int nThread = 10;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(nThread);

        new ThreadInsertTest2(nThread,startGate,endGate).start();
    }

    public void start() {
        for (int i=0;i<nThread;i++) {
            Thread thread = new Thread(new insert());
            thread.start();
        }
        long startTime = System.currentTimeMillis();
        startGate.countDown();
        try {
            endGate.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("插入3000万条数据用时: " + (endTime- startTime) + "ms");
    }




    public ThreadInsertTest2(int nThread, CountDownLatch startGate, CountDownLatch endGate) {
        this.nThread = nThread;
        this.startGate = startGate;
        this.endGate = endGate;
    }

    class insert implements Runnable {
        public void run() {
            try {
                startGate.await();
                Connection conn = null;
                PreparedStatement ps = null;
                String sql = "INSERT INTO user (user_name,password,sex) VALUE ('username','password',1)";
                try {
                    Class.forName("com.mysql.jdbc.Driver");
                    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/user?characterEncoding=utf8&useSSL=true&useServerPrepStmts=false&rewriteBatchedStatements=true", "root", "zqd19931007");
                    conn.setAutoCommit(false);
                    ps = conn.prepareStatement(sql);
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 3000000; i++) {
                        ps.addBatch(sql);
                        if (i % 100000 == 0) {
                            ps.executeBatch(); //每十万条数据提交一次，以防内存溢出
                            conn.commit();
                        }
                    }
                    ps.executeBatch(); //最后再提交一次，若插入数据不是整数也没有关系
                    conn.commit();
                    long end = System.currentTimeMillis();
                    System.out.println("单个线程插入数据所需时间：" + (end - start) + "ms");

                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
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
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                endGate.countDown();
            }
        }

    }
}
