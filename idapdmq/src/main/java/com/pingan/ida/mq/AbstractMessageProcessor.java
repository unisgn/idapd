package com.pingan.ida.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public class AbstractMessageProcessor implements MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageProcessor.class);
    protected String pid;
    protected int batchSize = 256;
    protected long timeout = 1;

    protected volatile int state = 0;
    protected int INIT = 1;
    protected int RUNNING = 2;
    protected int STOPPING = 3;
    protected int STOPPED = 4;

    protected int mqSize = 12000;
    protected int mqThres = 10000;

    protected int tqSize = 1200;

    protected BlockingQueue<Map> queue;
    protected ExecutorService executor;
    protected MessageRepository repository;

    protected int corePoolSize;
    protected int maxPoolSize;

    public AbstractMessageProcessor(String pid) {
        this.pid = pid;
        state = INIT;
        queue = new ArrayBlockingQueue<Map>(mqSize);
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void run() {
        logger.info("start run message processor#" + pid);
        state = RUNNING;
        for (;keepRunning();) {
            final List<Map> msgList = new ArrayList<Map>(batchSize);
            Map msg;
            for (int i = 0; i < batchSize; i++) {
                try {
                    msg = queue.poll(timeout, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        msgList.add(msg);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (msgList.size() > 0) {
                try {
                    executor.execute(new Runnable() {
                        public void run() {
                            repository.save(msgList);
                        }
                    });
                } catch (RejectedExecutionException e) {
                    handleRejectedExecutionException(msgList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private boolean keepRunning() {
        return state == RUNNING || (state == STOPPING && !queue.isEmpty());
    }

    public void stop() {
        logger.info("stopping processor#" + pid);
        state = STOPPING; // signal offer message off.
        for (;!queue.isEmpty(); ) {
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        state = STOPPED; // signal main run loop break.
        logger.info("processor#" + pid + " is stopped.");
    }

    private void handleRejectedExecutionException(final List<Map> msgList) {
        new Thread(new Runnable() {
            public void run() {
                for (Map item : msgList) {
                    try {
                        queue.put(item);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void offer(Map msg) {
        if (state < STOPPING) {
            try {
                queue.put(msg);
                if (queue.size() > mqThres) {
                    TimeUnit.SECONDS.sleep(10);
                }
            } catch (Exception e) {

            }
        } else {
            logger.info("message aborted since processor is no more running.");
        }
    }

}
