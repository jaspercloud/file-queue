package io.github.jaspercloud.filequeue;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StorageLock {

    private Lock writeLock = new ReentrantLock();
    private Lock readLock = new ReentrantLock();
    private Condition condition = writeLock.newCondition();

    public Lock getWriteLock() {
        return writeLock;
    }

    public Lock getReadLock() {
        return readLock;
    }

    public void signalAll() {
        try {
            writeLock.lock();
            condition.signalAll();
        } finally {
            writeLock.unlock();
        }
    }

    public void await() throws InterruptedException {
        try {
            writeLock.lock();
            condition.await();
        } finally {
            writeLock.unlock();
        }
    }

    public StorageLock() {
    }
}
