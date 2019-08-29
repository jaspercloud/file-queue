package io.github.jaspercloud.filequeue;

import io.github.jaspercloud.filequeue.util.MappedByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class QueueFileWriter {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private File file;
    private RandomAccessFile raf;
    private FileChannel channel;
    private MappedByteBuffer writer;

    public File getFile() {
        return file;
    }

    public MappedByteBuffer getWriteBuffer() {
        return writer;
    }

    public QueueFileWriter(File file, long maxSize) throws Exception {
        this.file = file;
        boolean exists = file.exists();
        if (!exists) {
            DBFile.writeMeta(file, maxSize);
        } else {
            maxSize = DBFile.readMeta(file);
        }
        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
        writer = channel.map(FileChannel.MapMode.READ_WRITE, Long.BYTES, maxSize);
    }

    public void writeRecord(byte[] bytes) {
        write(DBFile.Record, bytes);
    }

    public void writeEnd() {
        write(DBFile.End, new byte[0]);
    }

    public void write(int type, byte[] bytes) {
        MappedByteBufferUtil.write(writer, type, bytes);
    }

    public void flush() {
        writer.force();
    }

    public void close() {
        if (null != writer) {
            try {
                writer.force();
            } catch (Exception e) {
            }
        }
        if (null != channel) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (null != raf) {
            try {
                raf.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        try {
            MappedByteBufferUtil.cleanBuffer(writer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
