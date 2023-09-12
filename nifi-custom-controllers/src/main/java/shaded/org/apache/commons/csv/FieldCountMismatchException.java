package shaded.org.apache.commons.csv;

public class FieldCountMismatchException extends RuntimeException {

    public FieldCountMismatchException() {
    }

    public FieldCountMismatchException(String message) {
        super(message);
    }

    public FieldCountMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public FieldCountMismatchException(Throwable cause) {
        super(cause);
    }

    public FieldCountMismatchException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
