package com.pingan.ida.mq.util;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by franCiS on Feb 12, 2015.
 */
public class MessageReporter {

    private static volatile long startTime;
    private static  int totalSaved = 0;
    private static int totalReceived = 0;
    private static long cpuTime = 0;
    private static long ioTime = 0;

    public synchronized static void countTotalSaved(int count) {
        totalSaved += count;
    }

    public synchronized static void countCpuTime(long time) {
        cpuTime += time;
    }

    public synchronized static void countIoTime(long time) {
        ioTime += time;
    }

    public static int getTotalSaved() {
        return totalSaved;
    }

    public synchronized static void countTotalReceived() {
        totalReceived++;
    }

    public synchronized static void countTimer() {
        count++;
    }

    private static int count = 0;

    public static int getTotalReceived() {
        return totalReceived;
    }

    public static void setStartTime(long start) {
        startTime = start;
    }

    public static void report() {
        long elapse = System.currentTimeMillis();
        System.out.println("Total Rx  : " + totalReceived);
        System.out.println("Total Save: " + totalSaved);
        System.out.println("Avg   Rx  : " + ((totalReceived) * 1000 / (elapse - startTime)));
        System.out.println("Avg   Save: " + ((totalSaved) * 1000 / (elapse - startTime)));
        System.out.println("CPU   Time: " + Math.floor(cpuTime /100000/(count == 0 ? 1 : count)));
        System.out.println("IO    Time: " + Math.floor(ioTime/100000/(count == 0 ? 1 : count)));
        System.out.println("====================");
    }

    private static Timer timer = new Timer("reporter");

    public static void start() {
        timer.schedule(new TimerTask() {
            public void run() {
                MessageReporter.report();
            }
        }, 0, 5000);
    }

    public static void stop() {
        timer.cancel();
    }

}
