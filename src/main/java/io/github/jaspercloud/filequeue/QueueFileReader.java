package io.github.jaspercloud.filequeue;

import io.github.jaspercloud.filequeue.exception.RecordParseException;
import io.github.jaspercloud.filequeue.util.MappedByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class QueueFileReader {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private File file;
    private QueueFileWriter queueFileWriter;
    private RandomAccessFile raf;
    private FileChannel channel;
    private MappedByteBuffer reader;

    public File getFile() {
        return file;
    }

    public MappedByteBuffer getReaderBuffer() {
        return reader;
    }

    public QueueFileReader(QueueFileWriter queueFileWriter) throws Exception {
        this.queueFileWriter = queueFileWriter;
        file = queueFileWriter.getFile();
        boolean exists = queueFileWriter.getFile().exists();
        if (!exists) {
            throw new FileNotFoundException();
        }
        raf = new RandomAccessFile(file, "r");
        long maxSize = DBFile.readMeta(file);
        channel = raf.getChannel();
        reader = channel.map(FileChannel.MapMode.READ_ONLY, Long.BYTES, maxSize);
    }

    public byte[] readRecord() throws Exception {
        do {
            try {
                byte[] bytes = readOne();
                return bytes;
            } catch (RecordParseException e) {
                logger.error(e.getMessage(), e);
            }
        } while (true);
    }

    public byte[] readOne() throws Exception {
        byte[] bytes = MappedByteBufferUtil.read(reader, queueFileWriter.getWriteBuffer().position());
        return bytes;
    }

    public void close() {
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
            MappedByteBufferUtil.cleanBuffer(reader);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
