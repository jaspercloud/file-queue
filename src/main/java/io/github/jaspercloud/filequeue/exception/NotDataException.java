package io.github.jaspercloud.filequeue.exception;

public class NotDataException extends RuntimeException {

    public NotDataException() {
    }

    public NotDataException(String message) {
        super(message);
    }

    public NotDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotDataException(Throwable cause) {
        super(cause);
    }
}
