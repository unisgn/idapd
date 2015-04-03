package com.pingan.ida.mq.processor;

import com.pingan.ida.mq.AbstractMessageProcessor;
import com.pingan.ida.mq.repository.MessageRepository1001;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public class MessageProcessor1001 extends AbstractMessageProcessor {
    public MessageProcessor1001(String id) {
        super(id);
        repository = new MessageRepository1001();
    }
}
