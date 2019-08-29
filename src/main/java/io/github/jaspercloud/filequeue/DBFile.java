package io.github.jaspercloud.filequeue;

import org.springframework.util.FileCopyUtils;
import sun.misc.Cleaner;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.util.zip.CRC32;

public class DBFile {

    public static final byte[] Magic = "lfm".getBytes(Charset.forName("utf-8"));
    public static final int Record = 1;
    public static final int End = 2;

    private File file;
    private StorageLock storageLock;
    private QueueFileWriter writer;
    private QueueFileReader reader;

    public File getFile() {
        return file;
    }

    public QueueFileWriter getWriter() {
        return writer;
    }

    public QueueFileReader getReader() {
        return reader;
    }

    public DBFile(File file, long maxSize, StorageLock storageLock) throws Exception {
        this.file = file;
        this.storageLock = storageLock;
        writer = new QueueFileWriter(file, maxSize);
        reader = new QueueFileReader(writer);
    }

    public void putRecord(byte[] bytes) {
        writer.writeRecord(bytes);
    }

    public byte[] readRecord() throws Exception {
        byte[] bytes = reader.readRecord();
        return bytes;
    }

    public void endFile() {
        try {
            storageLock.getWriteLock().lock();
            writer.writeEnd();
            writer.flush();
            storageLock.signalAll();
        } finally {
            storageLock.getWriteLock().unlock();
        }
    }

    public void flush() {
        writer.flush();
    }

    public static long checkSum(byte[] bytes) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        long sum = crc32.getValue();
        return sum;
    }

    public static void writeMeta(File file, long len) throws Exception {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byteBuffer.putLong(len);
        byteBuffer.flip();
        FileCopyUtils.copy(byteBuffer.array(), file);
    }

    public static long readMeta(File file) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long len = raf.readLong();
            return len;
        }
    }

    public void close() {
        if (null != writer) {
            try {
                writer.close();
            } catch (Exception e) {
            }
        }
        if (null != reader) {
            try {
                reader.close();
            } catch (Exception e) {
            }
        }
    }
}
