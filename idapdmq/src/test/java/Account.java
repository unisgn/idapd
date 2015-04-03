import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by franCiS on Mar 07, 2015.
 */
public class Account {
    ReentrantLock lock = new ReentrantLock();
    private String name;
    private int balance;
    private Random random = new Random(System.currentTimeMillis());

    public Account(String name, int bal) {
        this.name = name;
        balance = bal;
    }

    public void  deposit(int amt) {

        lock.lock();
        int temp = balance;
        temp += amt;
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        balance = temp;
        lock.unlock();
    }
    public void withdraw(int amt) {
        lock.lock();
        int temp = balance;
        temp -= amt;
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        balance = temp;
        lock.unlock();
    }

    public int getBalance() {
        return balance;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            test();
        }
    }

    static void test() {

        final Account acct = new Account("acct", 1000);
        int NO = 1000;
        Thread[] threads = new Thread[NO];

        for (int i = 0; i < NO; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    acct.deposit(100);
                    acct.withdraw(100);
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < NO; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("final balance is: " + acct.getBalance());
    }
}
