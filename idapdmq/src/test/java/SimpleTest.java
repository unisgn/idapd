import com.pingan.ida.mq.util.MessageReporter;

import java.util.concurrent.TimeUnit;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public class SimpleTest {
    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
        MessageReporter.setStartTime(startTime);
        MessageGenerator generator = new MessageGenerator();
        generator.startup();
        MessageReporter.start();
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        generator.stop();
        try {
            TimeUnit.SECONDS.sleep(30);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MessageReporter.stop();
    }
}
