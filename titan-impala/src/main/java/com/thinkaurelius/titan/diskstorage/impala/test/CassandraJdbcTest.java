package com.thinkaurelius.titan.diskstorage.impala.test;

import org.apache.cassandra.cql.jdbc.CassandraDriver;

import java.sql.*;

/**
 * Created by Great on 2017/3/31.
 */
public class CassandraJdbcTest {
    private static final String CASSANDRA_URL = "jdbc:cassandra://127.0.0.1:9160/titan";

    public static void main(String[] args) {
        try {
            Driver cDriver = new CassandraDriver();
            DriverManager.registerDriver(cDriver);
            Connection con = DriverManager.getConnection(CASSANDRA_URL);
            String query = "select * from titan.titan_ids";
            Statement statement = con.createStatement();
            ResultSet rs = statement.executeQuery(query);
            int count = 0;
            while(rs.next()) {
                count++;
                byte [] key = rs.getBytes("key"),
                        column = rs.getBytes("column1"),
                        value = rs.getBytes("value");
                System.out.println(bytesToHexString(key) + "\t" + bytesToHexString(column) + "\t" + bytesToHexString(value));
            }
            System.out.println("(" + count + " rows)");
            rs.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String bytesToHexString(byte[] b) {
        StringBuffer sb = new StringBuffer();
        sb.append("0x");
        for(int i = 0; i < b.length; i++) {
            sb.append(Integer.toHexString(b[i]));
        }
        return sb.toString();
    }
}
