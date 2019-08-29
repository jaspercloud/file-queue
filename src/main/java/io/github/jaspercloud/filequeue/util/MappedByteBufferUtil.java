package io.github.jaspercloud.filequeue.util;

import io.github.jaspercloud.filequeue.DBFile;
import io.github.jaspercloud.filequeue.exception.RecordParseException;
import io.github.jaspercloud.filequeue.exception.NotDataException;
import io.github.jaspercloud.filequeue.exception.NotSpaceException;
import io.github.jaspercloud.filequeue.exception.WaitDataException;
import sun.misc.Cleaner;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class MappedByteBufferUtil {

    private static Method cleanerMethod;

    static {
        try {
            Class<?> clazz = Class.forName("java.nio.DirectByteBuffer");
            cleanerMethod = clazz.getMethod("cleaner", new Class[0]);
            cleanerMethod.setAccessible(true);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private MappedByteBufferUtil() {

    }

    public static void write(MappedByteBuffer mapBuffer, int type, byte[] bytes) {
        //header(magic+type)+(checkSum+len+data)
        long unUsed = mapBuffer.capacity() - mapBuffer.position();
        ByteBuffer dataBuffer = ByteBuffer.allocate(DBFile.Magic.length + Integer.BYTES
                + Long.BYTES + Integer.BYTES + bytes.length);
        //预留endBytes
        if (!Objects.equals(DBFile.End, type)) {
            if ((unUsed - dataBuffer.capacity()) < dataBuffer.capacity()) {
                throw new NotSpaceException();
            }
        }
        //magic
        dataBuffer.put(DBFile.Magic);
        //type
        dataBuffer.putInt(type);
        //checkSum
        dataBuffer.putLong(DBFile.checkSum(bytes));
        //len
        dataBuffer.putInt(bytes.length);
        //data
        dataBuffer.put(bytes);
        dataBuffer.flip();
        mapBuffer.put(dataBuffer);
    }

    public static byte[] read(MappedByteBuffer mapBuffer, int limit) {
        loadMagic(mapBuffer, limit);
        long crc32 = mapBuffer.getLong();
        int len = mapBuffer.getInt();
        byte[] bytes = new byte[len];
        mapBuffer.get(bytes);
        long sum = DBFile.checkSum(bytes);
        if (!Objects.equals(crc32, sum)) {
            throw new RecordParseException("crc32");
        }
        return bytes;
    }

    private static void loadMagic(MappedByteBuffer mapBuffer, int limit) {
        byte[] loadMagic = new byte[DBFile.Magic.length];
        int pos = mapBuffer.position();
        do {
            int headerLen = loadMagic.length + Integer.BYTES;
            if ((pos + headerLen) >= limit) {
                throw new WaitDataException();
            }
            if ((pos + headerLen) >= mapBuffer.capacity()) {
                throw new NotDataException();
            }
            mapBuffer.position(pos);
            mapBuffer.get(loadMagic, 0, loadMagic.length);
            pos += 1;
        } while (!Arrays.equals(DBFile.Magic, loadMagic));
        int type = mapBuffer.getInt();
        if (Objects.equals(DBFile.End, type)) {
            throw new NotDataException();
        }
    }

    public static void cleanBuffer(MappedByteBuffer buffer) throws Exception {
        if (null == buffer) {
            return;
        }
        Cleaner cleaner = (Cleaner) cleanerMethod.invoke(buffer, new Object[0]);
        cleaner.clean();
    }

}
