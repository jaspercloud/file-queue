package io.github.jaspercloud.filequeue.serializer;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class HessianSerializer<T> implements Serializer<T> {

    private Class<T> type;

    public HessianSerializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public Class<T> type() {
        return type;
    }

    @Override
    public byte[] encode(T obj) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        HessianOutput hessianOutput = new HessianOutput(stream);
        hessianOutput.writeObject(obj);
        byte[] bytes = stream.toByteArray();
        return bytes;
    }

    @Override
    public T decode(byte[] bytes) throws Exception {
        HessianInput hessianInput = new HessianInput(new ByteArrayInputStream(bytes));
        T bean = (T) hessianInput.readObject(type);
        return bean;
    }
}
