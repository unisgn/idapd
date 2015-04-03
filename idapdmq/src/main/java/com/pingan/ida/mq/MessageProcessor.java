package com.pingan.ida.mq;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public interface MessageProcessor extends Runnable {
    public void offer(Map msg);
    public void setExecutor(ExecutorService executor);
    public void stop();
}
