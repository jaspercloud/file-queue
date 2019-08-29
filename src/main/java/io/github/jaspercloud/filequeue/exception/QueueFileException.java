package io.github.jaspercloud.filequeue.exception;

public class QueueFileException extends RuntimeException {

    public QueueFileException() {
    }

    public QueueFileException(String message) {
        super(message);
    }

    public QueueFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueFileException(Throwable cause) {
        super(cause);
    }
}
