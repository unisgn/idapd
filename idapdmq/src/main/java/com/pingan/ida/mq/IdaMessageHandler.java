package com.pingan.ida.mq;

import com.pingan.ida.mq.util.MessageReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by franCiS on Feb 11, 2015.
 */
public class IdaMessageHandler {
    private volatile boolean terminated = false;
    private static final Logger logger = LoggerFactory.getLogger(IdaMessageHandler.class);
    private static MessageProcessorManager mgr = MessageProcessorManager.getInstance();

    public void handle(Map msg) {
        if (!terminated && msg != null) {
            MessageReporter.countTotalReceived();
            String pid = (String) msg.get("pid");
            mgr.proc(pid, msg);
        }
    }

    public void terminate() {
        logger.info("IdaMessageHandler is terminating...");
        terminated = true;
        mgr.shutdown();
        logger.info("IdaMessageHandler is terminated.");

    }
}
