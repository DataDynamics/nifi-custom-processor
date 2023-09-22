package io.datadynamics.nifi.reporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@Tags({"dd", "custom", "monitor", "memory", "usage", "heap", "jvm", "gc", "garbage collection", "warning"})
@CapabilityDescription("JVM에서 사용 가능한 Java 힙의 양을 확인합니다. 사용된 공간의 양이 구성 가능한 일부 임계값을 초과하는 경우 로그 메시지 및 시스템 수준 게시판을 통해 메모리 풀이 이 임계값을 초과한다고 경고합니다.")
public class MonitorMemoryUsageReportingTask extends AbstractReportingTask {

    public static final Pattern PERCENTAGE_PATTERN = Pattern.compile("\\d{1,2}%");

    public static final Pattern DATA_SIZE_PATTERN = DataUnit.DATA_SIZE_PATTERN;
    public static final PropertyDescriptor THRESHOLD_PROPERTY = new PropertyDescriptor.Builder()
            .name("메모리 사용율")
            .displayName("메모리 사용율")
            .description("경고를 생성하는 임계값을 나타냅니다. 백분율 또는 데이터 크기일 수 있습니다.")
            .required(true)
            .addValidator(new ThresholdValidator())
            .defaultValue("65%")
            .build();
    public static final PropertyDescriptor REPORTING_INTERVAL = new PropertyDescriptor.Builder()
            .name("리포팅 간격")
            .displayName("리포팅 간격")
            .description("설정한 메모리 사용율 임계값을 초과하는 경우 Bulletin에 레포팅하는 간격을 설정합니다. (예; 2000 nanos, 2000 millis, 20 secs, 5 mins, 1 hrs, 1 days)")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue(null)
            .build();
    public static final PropertyDescriptor EXTERNAL_HTTP_URL = new PropertyDescriptor.Builder()
            .name("외부에 통보할 HTTP URL")
            .description("외부 서비스에 HTTP URL을 호출하여 정보를 전달합니다.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor EXTERNAL_HTTP_URL_ENABLE = new PropertyDescriptor.Builder()
            .name("HTTP URL 통보 여부")
            .description("Alert에 디렉터리에 대해 표시할 이름입니다.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .dependsOn(EXTERNAL_HTTP_URL)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor HTTP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("HTTP Connection 타임아웃")
            .description("원격 서비스 연결을 위한 최대 대기 시간입니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    public static final PropertyDescriptor HTTP_WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("HTTP Write 타임아웃")
            .description("원격 서비스가 전송한 요청을 읽는 데 걸리는 최대 대기 시간입니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    private final static List<PropertyDescriptor> propertyDescriptors;
    public static ObjectMapper mapper = new ObjectMapper();

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(THRESHOLD_PROPERTY);
        _propertyDescriptors.add(REPORTING_INTERVAL);
        _propertyDescriptors.add(EXTERNAL_HTTP_URL_ENABLE);
        _propertyDescriptors.add(EXTERNAL_HTTP_URL);
        _propertyDescriptors.add(HTTP_CONNECTION_TIMEOUT);
        _propertyDescriptors.add(HTTP_WRITE_TIMEOUT);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
    }

    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();

    private volatile String threshold = "80%";

    private volatile long calculatedThreshold;

    private volatile long lastReportTime;

    private volatile long reportingIntervalMillis;

    private volatile boolean lastValueWasExceeded;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final String thresholdValue = context.getProperty(THRESHOLD_PROPERTY).getValue().trim();
        threshold = thresholdValue;

        final Long reportingIntervalValue = context.getProperty(REPORTING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        if (reportingIntervalValue == null) {
            reportingIntervalMillis = context.getSchedulingPeriod(TimeUnit.MILLISECONDS);
        } else {
            reportingIntervalMillis = reportingIntervalValue;
        }

        /////////////////////////////////////////
        // External HTTP Service
        /////////////////////////////////////////

        httpClientReference.set(null);

        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        Long connectTimeout = context.getProperty(HTTP_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        Long writeTimeout = context.getProperty(HTTP_WRITE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);

        // Set timeouts
        okHttpClientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
        okHttpClientBuilder.writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);

        httpClientReference.set(okHttpClientBuilder.build());
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final boolean isExternalHttpUrlEnable = context.getProperty(EXTERNAL_HTTP_URL_ENABLE).asBoolean();
        final String externalHttpUrl = context.getProperty(EXTERNAL_HTTP_URL).getValue();

        final String thresholdValue = context.getProperty(THRESHOLD_PROPERTY).getValue().trim();
        threshold = thresholdValue;

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

        if (DATA_SIZE_PATTERN.matcher(thresholdValue).matches()) {
            calculatedThreshold = DataUnit.parseDataSize(thresholdValue, DataUnit.B).longValue();
        } else {
            final String percentage = thresholdValue.substring(0, thresholdValue.length() - 1);
            final double pct = Double.parseDouble(percentage) / 100D;
            calculatedThreshold = (long) (heapMemoryUsage.getCommitted() * pct);
        }

        if (heapMemoryUsage.getUsed() > calculatedThreshold) {
            lastReportTime = System.currentTimeMillis();
            lastValueWasExceeded = true;

            /////////////////////////////////////////
            // Get Memory Usage
            /////////////////////////////////////////

            Map params = new HashMap();
            params.put("hostname", getHostname());
            params.put("type", "JVMHeapUsage");
            params.put("threshold", thresholdValue);
            params.put("calculatedThreshold", calculatedThreshold);
            params.put("max", memoryMXBean.getHeapMemoryUsage().getMax());
            params.put("used", memoryMXBean.getHeapMemoryUsage().getUsed());
            params.put("init", memoryMXBean.getHeapMemoryUsage().getInit());
            params.put("committed", memoryMXBean.getHeapMemoryUsage().getCommitted());

            getLogger().info("JVM Heap Memory Reporting Task : {}", params);

            /////////////////////////////////////////
            // External HTTP Service
            /////////////////////////////////////////

            if (isExternalHttpUrlEnable) {
                try {
                    String json = mapper.writeValueAsString(params);
                    final RequestBody requestBody = RequestBody.create(json, MediaType.parse("application/json"));

                    Request.Builder requestBuilder = new Request.Builder()
                            .post(requestBody)
                            .url(externalHttpUrl);

                    final Request request = requestBuilder
                            .addHeader("Content-Type", "application/json")
                            .build();

                    final OkHttpClient httpClient = httpClientReference.get();

                    final Call call = httpClient.newCall(request);
                    try (final Response response = call.execute()) {
                        if (!response.isSuccessful()) {
                            getLogger().warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s, Status Code : %s, Response Body : %s", externalHttpUrl, response.code(), response.body().string()));
                        }
                    }
                } catch (Exception e) {
                    getLogger().warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s", externalHttpUrl), e);
                }
            }
        } else if (lastValueWasExceeded) {
            lastValueWasExceeded = false;
            lastReportTime = System.currentTimeMillis();
        }
    }

    @OnStopped
    public void onStopped() {
    }

    private String getHostname() throws ProcessException {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("서버의 호스트명을 확인할 수 없습니다.", e);
        }
    }

    private static class ThresholdValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {

            if (!PERCENTAGE_PATTERN.matcher(input).matches() && !DATA_SIZE_PATTERN.matcher(input).matches()) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Valid value is a number in the range of 0-99 followed by a percent sign (e.g. 65%) or a Data Size (e.g. 100 MB)").build();
            }

            return new ValidationResult.Builder().input(input).subject(subject).valid(true).build();
        }
    }
}
