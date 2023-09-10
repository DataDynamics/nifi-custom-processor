package io.datadynamics.nifi;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.nifi.bootstrap.notification.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CustomNiFiNotificationService extends AbstractNotificationService {

    Logger logger = LoggerFactory.getLogger(AbstractNotificationService.class);

    public static ObjectMapper mapper = new ObjectMapper();

    public static final String NOTIFICATION_TYPE_KEY = "notification.type";
    public static final String NOTIFICATION_SUBJECT_KEY = "notification.subject";

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("The URL to send the notification to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection timeout")
            .description("Max wait time for connection to remote service.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();

    public static final PropertyDescriptor PROP_WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Write timeout")
            .description("Max wait time for remote service to read the request sent.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();

    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();
    private final AtomicReference<String> urlReference = new AtomicReference<>();

    private static final List<PropertyDescriptor> supportedProperties;

    static {
        supportedProperties = new ArrayList<>();
        supportedProperties.add(PROP_URL);
        supportedProperties.add(PROP_CONNECTION_TIMEOUT);
        supportedProperties.add(PROP_WRITE_TIMEOUT);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedProperties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

    @Override
    protected void init(final NotificationInitializationContext context) {
        final String url = context.getProperty(PROP_URL).evaluateAttributeExpressions().getValue();
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("[CustomNiFiNotificationService] Property, \"" + PROP_URL.getDisplayName() + "\", for the URL to POST notifications to must be set.");
        }

        urlReference.set(url);

        httpClientReference.set(null);

        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        Long connectTimeout = context.getProperty(PROP_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        Long writeTimeout = context.getProperty(PROP_WRITE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);

        // Set timeouts
        okHttpClientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
        okHttpClientBuilder.writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);

        httpClientReference.set(okHttpClientBuilder.build());
    }

    @Override
    public void notify(NotificationContext context, NotificationType notificationType, String subject, String message) throws NotificationFailedException {
        try {
            Map params = new HashMap();
            params.put("type", notificationType.name());
            params.put("subject", subject);
            params.put("message", message);
            params.put("properties", getProperties(context));
            params.put("jvmMetrics", getJvmMetrics(context));

            String json = mapper.writeValueAsString(params);
            logger.info("{}", String.format("Type : %s, Subject : %s, Message : %s, Detail : \n%s", notificationType.name(), subject, message, json));
            final RequestBody requestBody = RequestBody.create(json, MediaType.parse("application/json"));

            Request.Builder requestBuilder = new Request.Builder()
                    .post(requestBody)
                    .url(urlReference.get());

            final Request request = requestBuilder
                    .addHeader(NOTIFICATION_SUBJECT_KEY, subject)
                    .addHeader(NOTIFICATION_TYPE_KEY, notificationType.name())
                    .build();

            final OkHttpClient httpClient = httpClientReference.get();

            final Call call = httpClient.newCall(request);
            try (final Response response = call.execute()) {

                if (!response.isSuccessful()) {
                    throw new NotificationFailedException("[CustomNiFiNotificationService] Failed to send HTTP Notification. Received an unsuccessful status code response '" + response.code() + "'. The message was '" + response.message() + "'");
                }
            }
        } catch (NotificationFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new NotificationFailedException("[CustomNiFiNotificationService] Failed to send Http Notification", e);
        }
    }

    Map getProperties(NotificationContext context) {
        Map params = new HashMap();
        Map<PropertyDescriptor, String> configuredProperties = context.getProperties();
        for (PropertyDescriptor propertyDescriptor : configuredProperties.keySet()) {
            if (propertyDescriptor.isDynamic()) {
                String propertyValue = context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue();
                params.put(propertyDescriptor.getDisplayName(), propertyValue);
            }
        }
        return params;
    }

    Map getJvmMetrics(NotificationContext context) {
        Map params = new HashMap();

        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memBean.getHeapMemoryUsage();

        System.out.println(heapMemoryUsage.getMax()); // max memory allowed for jvm -Xmx flag (-1 if isn't specified)
        System.out.println(heapMemoryUsage.getCommitted()); // given memory to JVM by OS ( may fail to reach getMax, if there isn't more memory)
        System.out.println(heapMemoryUsage.getUsed()); // used now by your heap
        System.out.println(heapMemoryUsage.getInit()); // -Xms flag

        params.put("committedMemory", heapMemoryUsage.getCommitted());
        params.put("usedMemory", heapMemoryUsage.getUsed());
        params.put("maxMemory", heapMemoryUsage.getMax());
        params.put("initMemory", heapMemoryUsage.getInit());
        params.put("initMemory", heapMemoryUsage.getInit());
        params.put("activeThread", Thread.activeCount());
        params.put("totalThreadCount", ManagementFactory.getThreadMXBean().getThreadCount());
        params.put("threadDump", getThreadDump());

        return params;
    }

    private List getThreadDump() {
        List list = new ArrayList();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadMXBean.getAllThreadIds();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds);
        for (ThreadInfo threadInfo : threadInfos) {
            StringBuilder builder = new StringBuilder();
            builder.append("Thread name: " + threadInfo.getThreadName()).append("\n");
            builder.append("Thread state: " + threadInfo.getThreadState()).append("\n");
            builder.append("Thread stack trace: ").append("\n");
            for (StackTraceElement stackTraceElement : threadInfo.getStackTrace()) {
                builder.append("  " + stackTraceElement).append("\n");
            }
            list.add(builder.toString());
        }
        return list;
    }
}