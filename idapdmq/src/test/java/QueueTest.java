import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by franCiS on Mar 07, 2015.
 */
public class QueueTest {
    class Dat {
        BlockingQueue<String> queue = new ArrayBlockingQueue<String>(200);
        public void offer(String s) {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        int cnt = 0;
        BlockingQueue<String> queue = new ArrayBlockingQueue<String>(3);
        System.out.println(queue.size());
        for (int i = 0; i < 5; i++) {
            queue.put("s");
            cnt++;
        }
        System.out.println(cnt);
        System.out.println(queue.size());
    }
}

