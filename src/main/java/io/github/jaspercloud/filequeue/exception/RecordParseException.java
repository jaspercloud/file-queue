package io.github.jaspercloud.filequeue.exception;

public class RecordParseException extends RuntimeException {

    public RecordParseException() {
    }

    public RecordParseException(String message) {
        super(message);
    }

    public RecordParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecordParseException(Throwable cause) {
        super(cause);
    }
}
