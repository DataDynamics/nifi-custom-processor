package io.datadynamics.nifi.kudu;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database"})
public class DatabaseProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL select query")
            .description("The SQL select query to execute. The query can be empty, a constant value, or built from attributes "
                    + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                    + "incoming flowfiles. If this property is empty, the content of the incoming flow file is expected "
                    + "to contain a valid SQL select query, to be issued by the processor to the database. Note that Expression "
                    + "Language is not evaluated for flow file contents.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected DBCPService dbcpService;

    protected Set<Relationship> relationships;

    @Override
    public Set<Relationship> getRelationships() {
        relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    protected List<PropertyDescriptor> propDescriptors;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        propDescriptors = new ArrayList<>();
        propDescriptors.add(DBCP_SERVICE);
        propDescriptors.add(SQL_SELECT_QUERY);
        return propDescriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        String selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions().getValue();
        try {
            final Connection conn = dbcpService.getConnection();
            PreparedStatement psmt = conn.prepareStatement(selectQuery);
            ResultSet rs = psmt.executeQuery();
            while (rs.next()) {
                int value = rs.getInt(1);
                FlowFile flowFile = session.create();
                session.putAttribute(flowFile, "id", String.valueOf(value));
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            getLogger().warn("JDBC SQL을 실행할 수 없습니다. 메시지 : {}", e.getMessage(), e);

            session.transfer(session.create(), REL_FAILURE);
        }
    }
}