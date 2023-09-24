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
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CustomNiFiNotificationService extends AbstractNotificationService {

    public static final String NOTIFICATION_TYPE_KEY = "notification.type";
    public static final String NOTIFICATION_SUBJECT_KEY = "notification.subject";
    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("알람을 전송할 URL")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("커넥션 타임아웃")
            .description("원격 서비스에 접속을 위한 대기시간")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    public static final PropertyDescriptor PROP_WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("응답 대기시간")
            .description("원격 서비스에 전송한 요청의 응답을 대기하는 시간")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    public static final PropertyDescriptor PROP_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("JVM Heap & Thread 정보 수집")
            .description("JVM Heap Size 및 Thread 개수의 수집 여부")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    private static final List<PropertyDescriptor> supportedProperties;
    public static ObjectMapper mapper = new ObjectMapper();

    static {
        supportedProperties = new ArrayList<>();
        supportedProperties.add(PROP_URL);
        supportedProperties.add(PROP_CONNECTION_TIMEOUT);
        supportedProperties.add(PROP_WRITE_TIMEOUT);
        supportedProperties.add(PROP_JVM_METRICS);
    }

    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();
    private final AtomicReference<String> urlReference = new AtomicReference<>();
    Logger logger = LoggerFactory.getLogger(AbstractNotificationService.class);

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
        final boolean isCollectJVMMetrics = context.getProperty(PROP_JVM_METRICS).evaluateAttributeExpressions().asBoolean();

        try {
            Map params = new HashMap();
            params.put("hostname", InetAddress.getLocalHost().getHostName());
            params.put("type", notificationType.name());
            params.put("subject", subject);
            params.put("message", message);
            params.put("properties", getProperties(context));
            if (isCollectJVMMetrics) {
                params.put("jvmHeap", getJvmHeap());
                params.put("jvmThread", getThreadCount());
                params.put("jvmMetricsInclude", true);
            } else {
                params.put("jvmMetricsInclude", false);
            }

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
                    throw new NotificationFailedException("Failed to send HTTP Notification. Received an unsuccessful status code response '" + response.code() + "'. The message was '" + response.message() + "'");
                }
            }
        } catch (NotificationFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new NotificationFailedException("Failed to send Http Notification", e);
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

    public static Map getJvmHeap() {
        Map params = new HashMap();

        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memBean.getHeapMemoryUsage();

        params.put("committedMemory", heapMemoryUsage.getCommitted());
        params.put("usedMemory", heapMemoryUsage.getUsed());
        params.put("maxMemory", heapMemoryUsage.getMax());
        params.put("initMemory", heapMemoryUsage.getInit());

        return params;
    }

    public static Map getThreadCount() {
        Map params = new HashMap();
        params.put("activeThread", Thread.activeCount());
        params.put("peakThreadCount", ManagementFactory.getThreadMXBean().getPeakThreadCount());
        params.put("daemonThreadCount", ManagementFactory.getThreadMXBean().getDaemonThreadCount());
        params.put("currentThreadUserTime", ManagementFactory.getThreadMXBean().getCurrentThreadUserTime());
        params.put("currentThreadCpuTime", ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime());
        params.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());

        Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        Set<Thread> threadSet = threadMap.keySet();
        Map<String, AtomicInteger> threadGroupCountMap = new HashMap<>();
        for (Thread t : threadSet) {
            if (threadGroupCountMap.get(t.getThreadGroup().getName()) == null) {
                threadGroupCountMap.put(t.getThreadGroup().getName(), new AtomicInteger(0));
            }
            AtomicInteger count = threadGroupCountMap.get(t.getThreadGroup().getName());
            count.incrementAndGet();
        }

        Map<String, Integer> threadGroupActiveThreadCountMap = new HashMap<>();
        for (String key : threadGroupCountMap.keySet()) {
            AtomicInteger count = threadGroupCountMap.get(key);
            threadGroupActiveThreadCountMap.put(key, count.get());
        }

        params.put("activeThreadCountOfThreadGroup", threadGroupActiveThreadCountMap);
        return params;
    }

    public static List getFullThreadDump() {
        List list = new ArrayList();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadMXBean.getAllThreadIds();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds);
        for (ThreadInfo threadInfo : threadInfos) {
            StringBuilder builder = new StringBuilder();
            builder.append("threadName: " + threadInfo.getThreadName()).append("\n");
            builder.append("threadState: " + threadInfo.getThreadState()).append("\n");
            builder.append("threadStackTrace: ").append("\n");
            for (StackTraceElement stackTraceElement : threadInfo.getStackTrace()) {
                builder.append("  " + stackTraceElement).append("\n");
            }
            list.add(builder.toString());
        }
        return list;
    }
}