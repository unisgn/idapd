package com.pingan.ida.mq.repository;

import com.pingan.ida.mq.AbstractMessageRepository;
import com.pingan.ida.mq.MessageProcessorManager;
import com.pingan.ida.mq.util.MessageReporter;
import com.pingan.ida.mq.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public class MessageRepository1001 extends AbstractMessageRepository implements MessageRepository {

    private static final Logger logger = LoggerFactory.getLogger(MessageRepository1001.class);

    private class MsgComparator implements Comparator<Map> {
        String sorter;

        public MsgComparator(String sorter) {
            this.sorter = sorter;
        }

        public int compare(Map m1, Map m2) {
            return ((String) m1.get(sorter)).compareTo((String) m2.get(sorter));
        }
    }

    private final MsgComparator channelComparator = new MsgComparator("channel");
    private final MsgComparator branchComparator = new MsgComparator("branch");
    private final MsgComparator acctComparator = new MsgComparator("acct");

    /**
     * a ExecutorService used to handle batch update exception,
     * since exception means merely happen, better set corePoolSize to be minimum, like 1.
     */
    private final ExecutorService executorService = MessageProcessorManager.getInstance().getExecForErr();

    /**
     * the most headache thing in batch update might be the deadlock exception, which is unavoidable,
     * what you can do is to minimize the probability of deadlock.
     * one of the most effective way is to sort the batch of record before add to batch.
     * the sorters should be the combination of keywords which are used in UPDATE sql statement's WHERE clause
     * @param msgs the msgs to be saved
     */
    public void save(List<Map> msgs) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;

        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            sql = "UPDATE channel SET trans_cnt = trans_cnt + 1 WHERE channel = ?";
            pstmt = conn.prepareStatement(sql);
            Collections.sort(msgs, channelComparator);// usually we update a table by a set of keyword combination,
            for (Map item : msgs) {
                pstmt.setString(1, (String) item.get("channel"));
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            sql = "UPDATE branch SET trans_cnt = trans_cnt + 1 WHERE branch = ?";
            pstmt = conn.prepareStatement(sql);
            Collections.sort(msgs, branchComparator);
            for (Map item : msgs) {
                pstmt.setString(1, (String) item.get("branch"));
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            sql = "UPDATE acct_balance SET trans_cnt = trans_cnt + 1 WHERE acct = ?";
            pstmt = conn.prepareStatement(sql);
            Collections.sort(msgs, acctComparator);
            for (Map item : msgs) {
                pstmt.setString(1, (String) item.get("acct"));
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            conn.commit();
            conn.setAutoCommit(true);
            MessageReporter.countTotalSaved(msgs.size());
        } catch (BatchUpdateException e) {
            handleBatchException(msgs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * handler for batch update exception.
     * just split the batch of record into smaller pieces of batch record until the batch size reached to 1,
     * and update it in sub batches.
     * the most important param in such process is the sub batch size,
     * if too small, there will be too much sub batch pieces,
     * if too large, if failed again, which means too much sub sub batch pieces, and so on.
     *
     * @param msgs
     */
    private void handleBatchException(List<Map> msgs) {
        int len = msgs.size();
        logger.info("start batch update exception handler with batch size: " + len);

        if (len > 1) {
            int batchSize = (int) Math.floor(Math.sqrt(len)); // most efficient sub batch size

            for (int idx = 0, end; idx < len; idx += batchSize) {
                end = (idx + batchSize < len) ? (idx + batchSize) : len;

                final List<Map> subMsgs = new ArrayList<Map>(end - idx);
                for (int _idx = idx; _idx < end; _idx++) {
                    subMsgs.set(_idx - idx, msgs.get(_idx));
                }
                executorService.execute(new Runnable() {
                    public void run() {
                        save(subMsgs);
                    }
                });
            }
        } else { // len = 1
            logger.error("can't process msgs: " + msgs.toString());
        }
    }
}

