package io.github.jaspercloud.filequeue;

import io.github.jaspercloud.filequeue.serializer.HessianSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileQueueApplicationTests {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void contextLoads() throws Exception {
        File dir = new File("E:\\workspace\\fileLog");
//        30 * 60 * 1000
        FileBlockQueue<LogRecord> fileBlockQueue = new FileBlockQueue<>(dir, 10 * 1000, 10 * 1000, new HessianSerializer(LogRecord.class));
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        fileBlockQueue.put(new LogRecord(UUID.randomUUID().toString(), "test"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        for (int i = 0; i < 5; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            LogRecord logRecord = fileBlockQueue.take();
                            logger.info("log: thread={}, data={}", Thread.currentThread().getId(), logRecord.toString());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        System.out.println();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }

}
