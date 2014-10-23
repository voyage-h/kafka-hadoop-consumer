package org.conan.kafka;

import scala.collection.parallel.immutable.ParRange;

import java.security.PrivateKey;
import java.sql.*;

public class PhoenixUtil {
    private String _topic;
    private Long _offset;
    private Connection _con;

    public PhoenixUtil(String topic,Long offset) {
        _topic = topic;
        _offset = offset;
        try {
            _con = DriverManager.getConnection("jdbc:phoenix:zk-1,zk-2,zk-3");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void insert() throws SQLException {
        Statement stmt = null;
        stmt = _con.createStatement();
        //stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
        stmt.executeUpdate("UPSERT INTO GBILL VALUES (1,'Hello')");
        _con.commit();
        _con.close();

    }
}