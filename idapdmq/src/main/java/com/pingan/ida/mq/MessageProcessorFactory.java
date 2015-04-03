package com.pingan.ida.mq;

import com.pingan.ida.mq.processor.MessageProcessor1001;
import com.pingan.ida.mq.processor.MessageProcessor1002;

/**
 * Created by franCiS on Mar 12, 2015.
 */
public class MessageProcessorFactory {


    public static MessageProcessor gen(String pid) {
        if ("1001".equals(pid)) {
            return new MessageProcessor1001(pid);
        } else if ("1002".equals(pid)) {
            return new MessageProcessor1002(pid);
        } else {
            return null;
        }
    }

}
