package io.github.jaspercloud.filequeue.serializer;

public interface Serializer<T> {

    Class<T> type();

    byte[] encode(T obj) throws Exception;

    T decode(byte[] bytes) throws Exception;
}
