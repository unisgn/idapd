package com.pingan.ida.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by franCiS on Mar 12, 2015.
 */
public class MessageProcessorManager {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessorManager.class);
    private MessageProcessorManager() {}
    private static class SingletonFactory {
        private static MessageProcessorManager inst = new MessageProcessorManager();
    }

    public static MessageProcessorManager getInstance() {
        return SingletonFactory.inst;
    }

    private Map<String, MessageProcessor> processors = new HashMap<String, MessageProcessor>();
    private ThreadPoolExecutor exec = new ThreadPoolExecutor(1, 10, 60, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>());
    private ExecutorService executor = new ThreadPoolExecutor(2, 2, 60, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(500));
    private ExecutorService errExec = Executors.newFixedThreadPool(1);

    public boolean proc(String pid, Map msg) {
        if (processors.containsKey(pid)) {
            processors.get(pid).offer(msg);
            return true;
        } else {
            MessageProcessor processor = MessageProcessorFactory.gen(pid);
            if (processor != null) {
                processor.offer(msg);
                processors.put(pid, processor);
                processor.setExecutor(executor);
                exec.execute(processor);
                return true;
            } else {
                return false;
            }
        }
    }

    public void shutdown() {
        logger.info("shutting down message processor manager...");
        for (String pid : processors.keySet()) {
            processors.get(pid).stop();
        }
        executor.shutdown();
        for (;!executor.isTerminated();) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("message processor manager is down.");
    }

    public ExecutorService getExecForErr() {
        return errExec;
    }


}
