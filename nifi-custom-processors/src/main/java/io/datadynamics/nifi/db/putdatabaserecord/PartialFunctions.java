package io.datadynamics.nifi.db.putdatabaserecord;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;

/**
 * This class contains various partial functions those are reusable among process patterns.
 */
public class PartialFunctions {

    public static <FCT> FetchFlowFiles<FCT> fetchSingleFlowFile() {
        return (context, session, functionContext, result) -> session.get(1);
    }

    public static <FCT> TransferFlowFiles<FCT> transferRoutedFlowFiles() {
        return (context, session, functionContext, result)
                -> result.getRoutedFlowFiles().forEach(((relationship, routedFlowFiles)
                -> session.transfer(routedFlowFiles, relationship)));
    }

    /**
     * <p>This method is identical to what {@link org.apache.nifi.processor.AbstractProcessor#onTrigger(ProcessContext, ProcessSession)} does.</p>
     * <p>Create a session from ProcessSessionFactory and execute specified onTrigger function, and commit the session if onTrigger finishes successfully.</p>
     * <p>When an Exception is thrown during execution of the onTrigger, the session will be rollback. FlowFiles being processed will be penalized.</p>
     */
    public static void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger, OnTrigger onTrigger) throws ProcessException {
        onTrigger(context, sessionFactory, logger, onTrigger, (session, t) -> session.rollback(true));
    }

    public static void onTrigger(
            ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger, OnTrigger onTrigger,
            RollbackSession rollbackSession) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger.execute(session);
            session.commitAsync();
        } catch (final Throwable t) {
            logger.error("{} failed to process due to {}; rolling back session", new Object[]{onTrigger, t});
            rollbackSession.rollback(session, t);
            throw t;
        }
    }

    @FunctionalInterface
    public interface InitConnection<FC, C> {
        C apply(ProcessContext context, ProcessSession session, FC functionContext, List<FlowFile> flowFiles) throws ProcessException;
    }

    @FunctionalInterface
    public interface FetchFlowFiles<FC> {
        List<FlowFile> apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;
    }

    @FunctionalInterface
    public interface OnCompleted<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection) throws ProcessException;
    }

    @FunctionalInterface
    public interface OnFailed<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection, Exception e) throws ProcessException;
    }

    @FunctionalInterface
    public interface Cleanup<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection) throws ProcessException;
    }

    @FunctionalInterface
    public interface FlowFileGroup {
        List<FlowFile> getFlowFiles();
    }

    @FunctionalInterface
    public interface AdjustRoute<FC> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;
    }

    @FunctionalInterface
    public interface TransferFlowFiles<FC> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;

        default TransferFlowFiles<FC> andThen(TransferFlowFiles<FC> after) {
            return (context, session, functionContext, result) -> {
                apply(context, session, functionContext, result);
                after.apply(context, session, functionContext, result);
            };
        }
    }

    @FunctionalInterface
    public interface OnTrigger {
        void execute(ProcessSession session) throws ProcessException;
    }


    @FunctionalInterface
    public interface RollbackSession {
        void rollback(ProcessSession session, Throwable t);
    }

    @FunctionalInterface
    public interface AdjustFailed {
        boolean apply(ProcessContext context, RoutingResult result);
    }
}