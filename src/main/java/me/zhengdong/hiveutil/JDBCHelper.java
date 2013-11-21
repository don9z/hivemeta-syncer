package me.zhengdong.hiveutil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCHelper {
    public static String mysqlJDBCProtocol = "jdbc:mysql://";
    public static String hiveJDBCProtocol = "jdbc:hive://";

    private static void loadJDBCDriver(String driverName) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void loadMySQLJDBCDriver() {
        String mysqlDriverName = "com.mysql.jdbc.Driver";
        loadJDBCDriver(mysqlDriverName);
    }

    public static void loadHiveJDBCDriver() {
        String hiveDriverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
        loadJDBCDriver(hiveDriverName);
    }

    public static void close(ResultSet res, Statement stmt, Connection con) {
        if (res != null) {
            try {
                res.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
