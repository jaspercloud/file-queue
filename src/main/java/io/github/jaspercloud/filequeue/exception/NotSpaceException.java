package io.github.jaspercloud.filequeue.exception;

public class NotSpaceException extends RuntimeException {

    public NotSpaceException() {
    }

    public NotSpaceException(String message) {
        super(message);
    }

    public NotSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotSpaceException(Throwable cause) {
        super(cause);
    }
}
