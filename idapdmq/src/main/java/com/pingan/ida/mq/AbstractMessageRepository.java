package com.pingan.ida.mq;

import com.pingan.ida.mq.repository.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public class AbstractMessageRepository {
    protected Connection getConnection() throws SQLException {
        return ConnectionFactory.getConnection();
    }
}
