package io.datadynamics.nifi.db.putdatabaserecord;


import static io.datadynamics.nifi.db.putdatabaserecord.ErrorTypes.Destination.*;
import static io.datadynamics.nifi.db.putdatabaserecord.ErrorTypes.Penalty.*;

/**
 * Represents general error types and how it should be treated.
 */
public enum ErrorTypes {

    /**
     * Procedure setting has to be fixed, otherwise the same error would occur irrelevant to the input.
     * In order to NOT call failing process frequently, this should be yielded.
     */
    PersistentFailure(ProcessException, Yield),

    /**
     * It is unknown whether the error is persistent or temporal, related to the input or not.
     */
    UnknownFailure(ProcessException, None),

    /**
     * The input will be sent to the failure route for recovery without penalizing.
     * Basically, the input should not be sent to the same procedure again unless the issue has been solved.
     */
    InvalidInput(Failure, None),

    /**
     * The procedure is temporarily unavailable, usually due to the external service unavailability.
     * Retrying maybe successful, but it should be yielded for a while.
     */
    TemporalFailure(Retry, Yield),

    /**
     * The input was not processed successfully due to some temporal error
     * related to the specifics of the input. Retrying maybe successful,
     * but it should be penalized for a while.
     */
    TemporalInputFailure(Retry, Penalize),

    /**
     * The input was not ready for being processed. It will be kept in the incoming queue and also be penalized.
     */
    Defer(Self, Penalize);

    private final Destination destination;
    private final Penalty penalty;

    ErrorTypes(Destination destination, Penalty penalty) {
        this.destination = destination;
        this.penalty = penalty;
    }

    public Result result() {
        return new Result(destination, penalty);
    }

    public Destination destination() {
        return this.destination;
    }

    public Penalty penalty() {
        return this.penalty;
    }

    /**
     * Represents the destination of input.
     */
    public enum Destination {
        ProcessException, Failure, Retry, Self
    }

    /**
     * Indicating yield or penalize the processing when transfer the input.
     */
    public enum Penalty {
        Yield, Penalize, None
    }

    /**
     * Result represents a result of a procedure.
     * ErrorTypes enum contains basic error result patterns.
     */
    public static class Result {
        private final Destination destination;
        private final Penalty penalty;

        public Result(Destination destination, Penalty penalty) {
            this.destination = destination;
            this.penalty = penalty;
        }

        public Destination destination() {
            return destination;
        }

        public Penalty penalty() {
            return penalty;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "destination=" + destination +
                    ", penalty=" + penalty +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Result result = (Result) o;

            if (destination != result.destination) return false;
            return penalty == result.penalty;
        }

        @Override
        public int hashCode() {
            int result = destination != null ? destination.hashCode() : 0;
            result = 31 * result + (penalty != null ? penalty.hashCode() : 0);
            return result;
        }
    }

}