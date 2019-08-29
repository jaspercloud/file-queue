package io.github.jaspercloud.filequeue;

import io.github.jaspercloud.filequeue.serializer.Serializer;

import java.io.File;

public class FileBlockQueue<T> {

    private Serializer<T> serializer;
    private QueueStorage queueStorage;

    public FileBlockQueue(File storageDir, long fileSize, long flushTime, Serializer<T> serializer) throws Exception {
        this.serializer = serializer;
        this.queueStorage = new QueueStorage(storageDir, fileSize, flushTime);
    }

    public T take() throws Exception {
        byte[] bytes = queueStorage.take();
        T decode = serializer.decode(bytes);
        return decode;
    }

    public T get() throws Exception {
        byte[] bytes = queueStorage.get();
        if (null == bytes) {
            return null;
        }
        T decode = serializer.decode(bytes);
        return decode;
    }

    public void put(T t) throws Exception {
        byte[] encode = serializer.encode(t);
        queueStorage.put(encode);
    }
}
