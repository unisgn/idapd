import com.pingan.ida.mq.IdaMessageHandler;
import com.pingan.ida.mq.repository.ConnectionFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created by franCiS on Feb 11, 2015.
 */
public class MessageGenerator {
    private static Logger logger = LoggerFactory.getLogger(MessageGenerator.class);
    private List<String> list_acct = new ArrayList<String>();
    private List<String> list_ccy = new ArrayList<String>();
    private List<String> list_branch = new ArrayList<String>();
    private List<String> list_channel = new ArrayList<String>();

    private boolean errored = false;
    private boolean started = false;
    private boolean stopped = false;

    private Timer generator = new Timer("generator", true);

    private int SIZE_ACCT = 500;
    private int SIZE_CCY = 26;
    private int SIZE_BRANCH = 2000;
    private int SIZE_CHANNEL = 200;
    private DateTime beginTime = new DateTime(2015, 1, 1, 0, 0, 0);
    private DateTime endTime = new DateTime(2015, 3, 1, 0, 0, 0);
    private String dateFormat = "yyyy-MM-dd HH:mm:ss";
    private Random random = new Random(System.currentTimeMillis());

    private IdaMessageHandler handler = new IdaMessageHandler();


    private void init() {
        int idx, size;
        size = SIZE_ACCT;
        for (idx = 0; idx < size; idx++) {
            list_acct.add(generate_string(20, 0));
        }
        size = SIZE_CCY;
        for (idx = 0; idx < size; idx++) {
            list_ccy.add(generate_string(3, 1));
        }
        size = SIZE_BRANCH;
        for (idx = 0; idx < size; idx++) {
            list_branch.add(generate_string(6, 3));
        }
        size = SIZE_CHANNEL;
        for (idx = 0; idx < size; idx++) {
            list_channel.add(generate_string(4, 0));
        }

        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        String sql;
        try {
            conn = ConnectionFactory.getConnection();
            stmt = conn.createStatement();

            sql = "TRUNCATE TABLE branch;TRUNCATE TABLE channel;TRUNCATE TABLE acct_balance;TRUNCATE TABLE t_ccy";
            stmt.execute(sql);

            conn.setAutoCommit(false);

            sql = "INSERT INTO acct_balance (acct) VALUES (?)";
            pstmt = conn.prepareStatement(sql);
            for (String s : list_acct) {
                pstmt.setString(1, s);
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            sql = "INSERT INTO channel (channel) VALUES (?)";
            pstmt = conn.prepareStatement(sql);
            for (String s : list_channel) {
                pstmt.setString(1, s);
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            sql = "INSERT INTO branch (branch) VALUES (?)";
            pstmt = conn.prepareStatement(sql);
            for (String s : list_branch) {
                pstmt.setString(1, s);
                pstmt.addBatch();
            }
            pstmt.executeBatch();


            conn.commit();
            conn.setAutoCommit(true);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

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

    public void startup() {
        if (!started) {
            started = true;
            logger.info("startup message generator.");
            init();
            generator.schedule(new SendMsg(), 1000, 1000);
        }

    }

    public void stop() {
        if (started) {
            started = false; // now no more message will be sent.
            generator.cancel();
            logger.info("message generator stopped.");
            handler.terminate();

        }

    }

    private class SendMsg extends TimerTask {
        public void run() {
            if (started) {
                for (int i = 0, max = 500 + new Random(System.currentTimeMillis()).nextInt(5000); i < max && started; i++) {
                    handler.handle(generate_record());
                }
            }
        }
    }

    private Map<String, String> generate_record() {
        Map<String, String> ret = new HashMap<String, String>();
        ret.put("pid", "1001");
        ret.put("seq", String.valueOf(random.nextInt(999999999)));
        ret.put("acct", list_acct.get(random.nextInt(SIZE_ACCT)));
        ret.put("branch", list_branch.get(random.nextInt(SIZE_BRANCH)));
        ret.put("channel", list_channel.get(random.nextInt(SIZE_CHANNEL)));
        ret.put("ccy", list_ccy.get(random.nextInt(SIZE_CCY)));
        ret.put("amount", String.valueOf(random.nextInt(999999999)));
        ret.put("trans_date", generate_date(beginTime, endTime, dateFormat));
        return ret;
    }

    private Map<String, String> generate_invalid_record() {
        Map<String, String> ret = new HashMap<String, String>();
        ret.put("pid", "1001");
        ret.put("seq", String.valueOf(random.nextInt(999999999)));
        ret.put("acct", list_acct.get(random.nextInt(SIZE_ACCT)));
        ret.put("branch", "abcde12345abcde");
        ret.put("channel", list_channel.get(random.nextInt(SIZE_CHANNEL)));
        ret.put("ccy", list_ccy.get(random.nextInt(SIZE_CCY)));
        ret.put("amount", String.valueOf(random.nextInt(999999999)));
        ret.put("trans_date", generate_date(beginTime, endTime, dateFormat));
        return ret;
    }


    private String generate_string(int length, int mode) {
        String[] bases = {"abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
                "abcdefghijklmnopqrstuvwxyz0123456789", "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"};
        String base = bases[mode];
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    private String generate_date(DateTime begin, DateTime end, String format) {
        return new DateTime(begin.getMillis() + random.nextInt((int) (end.getMillis() - begin.getMillis()))).toString(format);
    }
}
