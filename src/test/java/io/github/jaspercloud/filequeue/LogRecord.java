package io.github.jaspercloud.filequeue;

import java.io.Serializable;

public class LogRecord implements Serializable {

    private String id;
    private String data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public LogRecord() {
    }

    public LogRecord(String id, String data) {
        this.id = id;
        this.data = data;
    }

    @Override
    public String toString() {
        return "LogRecord{" +
                "id='" + id + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
