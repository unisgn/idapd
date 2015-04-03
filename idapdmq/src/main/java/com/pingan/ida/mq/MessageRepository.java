package com.pingan.ida.mq;

import java.util.List;
import java.util.Map;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public interface MessageRepository {
    public void save(List<Map> msgList);
}
