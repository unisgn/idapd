package com.pingan.ida.mq.repository;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.*;

/**
 * Created by franCiS on Feb 11, 2015.
 */
public class ConnectionFactory {

    private static DataSource ds = new ComboPooledDataSource("demo");

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
