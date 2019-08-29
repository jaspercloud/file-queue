package io.github.jaspercloud.filequeue.exception;

public class WaitDataException extends RuntimeException {

    public WaitDataException() {
    }

    public WaitDataException(String message) {
        super(message);
    }

    public WaitDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public WaitDataException(Throwable cause) {
        super(cause);
    }
}
