package io.datadynamics.nifi.kudu;

import io.datadynamics.nifi.kudu.json.TimestampFormatHolder;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.Record;

import javax.security.auth.login.AppConfigurationEntry;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockPutKudu extends PutKudu {

    private KuduSession session;
    private LinkedList<Operation> opQueue;

    // Atomic reference is used as the set and use of the schema are in different thread
    private AtomicReference<Schema> tableSchema = new AtomicReference<>();

    private boolean loggedIn = false;
    private boolean loggedOut = false;

    public MockPutKudu() {
        this(mock(KuduSession.class));
    }

    public MockPutKudu(KuduSession session) {
        this.session = session;
        this.opQueue = new LinkedList<>();
    }

    public void queue(Operation... operations) {
        opQueue.addAll(Arrays.asList(operations));
    }

    @Override
    protected Operation createKuduOperation(OperationType operationType, Record record,
                                            List<String> fieldNames, boolean ignoreNull,
                                            boolean lowercaseFields, KuduTable kuduTable,
                                            TimestampFormatHolder holder,
                                            String defaultTimestampPatterns) {

        Operation operation = opQueue.poll();
        if (operation == null) {
            switch (operationType) {
                case INSERT:
                    operation = mock(Insert.class);
                    break;
                case INSERT_IGNORE:
                    operation = mock(InsertIgnore.class);
                    break;
                case UPSERT:
                    operation = mock(Upsert.class);
                    break;
                case UPDATE:
                    operation = mock(Update.class);
                    break;
                case UPDATE_IGNORE:
                    operation = mock(UpdateIgnore.class);
                    break;
                case DELETE:
                    operation = mock(Delete.class);
                    break;
                case DELETE_IGNORE:
                    operation = mock(DeleteIgnore.class);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("OperationType: %s not supported by Kudu", operationType));
            }
        }
        return operation;
    }

    @Override
    protected boolean supportsIgnoreOperations() {
        return true;
    }

    @Override
    public KuduClient buildClient(ProcessContext context) {
        final KuduClient client = mock(KuduClient.class);

        try {
            when(client.openTable(anyString())).thenReturn(mock(KuduTable.class));
        } catch (final Exception e) {
            throw new AssertionError(e);
        }

        return client;
    }

    @Override
    protected void executeOnKuduClient(Consumer<KuduClient> actionOnKuduClient) {
        final KuduClient client = mock(KuduClient.class);

        try {
            final KuduTable kuduTable = mock(KuduTable.class);
            when(client.openTable(anyString())).thenReturn(kuduTable);
            when(kuduTable.getSchema()).thenReturn(tableSchema.get());
        } catch (final Exception e) {
            throw new AssertionError(e);
        }

        actionOnKuduClient.accept(client);
    }

    public boolean loggedIn() {
        return loggedIn;
    }

    public boolean loggedOut() {
        return loggedOut;
    }

    @Override
    protected KerberosUser createKerberosKeytabUser(String principal, String keytab, ProcessContext context) {
        return createMockKerberosUser(principal);
    }

    @Override
    protected KerberosUser createKerberosPasswordUser(String principal, String password, ProcessContext context) {
        return createMockKerberosUser(principal);
    }

    private KerberosUser createMockKerberosUser(final String principal) {
        return new KerberosUser() {

            @Override
            public void login() {
                loggedIn = true;
            }

            @Override
            public void logout() {
                loggedOut = true;
            }

            @Override
            public <T> T doAs(final PrivilegedAction<T> action) throws IllegalStateException {
                return action.run();
            }

            @Override
            public <T> T doAs(PrivilegedAction<T> action, ClassLoader contextClassLoader) throws IllegalStateException {
                return action.run();
            }

            @Override
            public <T> T doAs(final PrivilegedExceptionAction<T> action) throws IllegalStateException, PrivilegedActionException {
                try {
                    return action.run();
                } catch (Exception e) {
                    throw new PrivilegedActionException(e);
                }
            }

            @Override
            public <T> T doAs(PrivilegedExceptionAction<T> action, ClassLoader contextClassLoader) throws IllegalStateException, PrivilegedActionException {
                try {
                    return action.run();
                } catch (Exception e) {
                    throw new PrivilegedActionException(e);
                }
            }

            @Override
            public boolean checkTGTAndRelogin() {
                return true;
            }

            @Override
            public boolean isLoggedIn() {
                return loggedIn && !loggedOut;
            }

            @Override
            public String getPrincipal() {
                return principal;
            }

            @Override
            public AppConfigurationEntry getConfigurationEntry() {
                return new AppConfigurationEntry("LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Collections.emptyMap());
            }
        };
    }

    @Override
    protected KuduSession createKuduSession(final KuduClient client) {
        return session;
    }

    void setTableSchema(final Schema tableSchema) {
        this.tableSchema.set(tableSchema);
    }
}