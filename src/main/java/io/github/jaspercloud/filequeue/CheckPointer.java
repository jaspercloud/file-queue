package io.github.jaspercloud.filequeue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CheckPointer {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private File file;
    private ReentrantLock lock = new ReentrantLock();

    public ReentrantLock getCheckPointLock() {
        return lock;
    }

    public CheckPointer(File file) {
        this.file = file;
    }

    public CheckPoint get() throws Exception {
        if (!file.exists()) {
            return null;
        }
        try {
            lock.lock();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            try (FileChannel channel = raf.getChannel()) {
                try (FileLock lock = channel.lock()) {
                    int len = raf.readInt();
                    byte[] bytes = new byte[len];
                    raf.read(bytes);
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    int nameLen = buffer.getInt();
                    byte[] name = new byte[nameLen];
                    buffer.get(name);
                    int pos = buffer.getInt();
                    CheckPoint checkPoint = new CheckPoint(new String(name), pos);
                    return checkPoint;
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public void save(CheckPoint point) throws Exception {
        try {
            lock.lock();
            byte[] name = point.getName().getBytes(Charset.forName("utf-8"));
            ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + name.length + Integer.BYTES);
            byteBuffer.putInt(name.length);
            byteBuffer.put(name);
            byteBuffer.putInt(point.getPos());
            byteBuffer.flip();
            byte[] bytes = byteBuffer.array();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            try (FileChannel channel = raf.getChannel()) {
                try (FileLock lock = channel.lock()) {
                    raf.writeInt(bytes.length);
                    raf.write(bytes);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static class CheckPoint {

        private String name;
        private int pos;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getPos() {
            return pos;
        }

        public void setPos(int pos) {
            this.pos = pos;
        }

        public CheckPoint() {
        }

        public CheckPoint(String name, int pos) {
            this.name = name;
            this.pos = pos;
        }
    }
}
