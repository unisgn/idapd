import com.pingan.ida.mq.repository.ConnectionFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by franCiS on Mar 05, 2015.
 */
public class BatchTest {
    public static void main(String[] args) throws SQLException {
        ExecutorService executorService = Executors.newFixedThreadPool(30);

        int size = 10000;
        long time = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;
        try {
            conn = ConnectionFactory.getConnection();
            time = System.nanoTime();
            sql = "UPDATE channel SET trans_cnt = trans_cnt + 1 WHERE channel = ?";
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < size; i++) {
                pstmt.setString(1, "ccep");
                pstmt.addBatch();
            }
            long startTime = System.nanoTime();
            pstmt.executeBatch();
            long endTime = System.nanoTime();
            System.out.println(Math.floor(1000000000 * ((long)size) / (endTime - startTime)));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

    }
}
