package io.datadynamics.nifi.reporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.FormatUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"dd", "custom", "disk", "storage", "warning", "monitoring", "repo"})
@CapabilityDescription("지정된 디렉토리에 사용 가능한 저장 공간의 양을 확인하고 해당 디렉토리가 있는 파티션이 구성 가능한 저장 공간 임계값을 초과하는 경우 로그 메시지 및 시스템 수준 Bulletin을 통해 경고합니다. 또한 외부 서비스에 해당 알림을 발송합니다.")
public class MonitorDiskUsageReportingTask extends AbstractReportingTask {

    public static final PropertyDescriptor DIR_LOCATION = new PropertyDescriptor.Builder()
            .name("디렉토리 경로")
            .description("모니터링할 파티션의 디렉토리 경로입니다.")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .build();
    public static final PropertyDescriptor DIR_DISPLAY_NAME = new PropertyDescriptor.Builder()
            .name("Alert에 표시할 디렉토리 명")
            .description("Alert에 디렉터리에 대해 표시할 이름입니다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("Un-Named")
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
    private static final Pattern PERCENT_PATTERN = Pattern.compile("(\\d+{1,2})%");
    public static final PropertyDescriptor DIR_THRESHOLD = new PropertyDescriptor.Builder()
            .name("임계치")
            .description("디렉터리가 있는 파티션의 디스크 사용량이 문제임을 나타내기 위해 Bulletin에 생성되는 임계값입니다.")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(PERCENT_PATTERN))
            .defaultValue("80%")
            .build();
    public static ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(2);
        descriptors.add(DIR_THRESHOLD);
        descriptors.add(DIR_LOCATION);
        descriptors.add(DIR_DISPLAY_NAME);
        descriptors.add(EXTERNAL_HTTP_URL_ENABLE);
        descriptors.add(EXTERNAL_HTTP_URL);
        descriptors.add(HTTP_CONNECTION_TIMEOUT);
        descriptors.add(HTTP_WRITE_TIMEOUT);
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
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
        final String thresholdValue = context.getProperty(DIR_THRESHOLD).getValue();
        final Matcher thresholdMatcher = PERCENT_PATTERN.matcher(thresholdValue.trim());
        thresholdMatcher.find();
        final String thresholdPercentageVal = thresholdMatcher.group(1);
        final int contentRepoThreshold = Integer.parseInt(thresholdPercentageVal);

        final File dir = new File(context.getProperty(DIR_LOCATION).getValue());
        final String dirName = context.getProperty(DIR_DISPLAY_NAME).getValue();
        final boolean isExternalHttpUrlEnable = context.getProperty(EXTERNAL_HTTP_URL_ENABLE).asBoolean();
        final String externalHttpUrl = context.getProperty(EXTERNAL_HTTP_URL).getValue();

        checkThreshold(dirName, dir.toPath(), contentRepoThreshold, getLogger(), isExternalHttpUrlEnable, externalHttpUrl);
    }

    void checkThreshold(final String pathName, final Path path, final int threshold, final ComponentLog logger, boolean isExternalHttpUrlEnable, String externalHttpUrl) {
        final File file = path.toFile();
        final long totalBytes = file.getTotalSpace();
        final long freeBytes = file.getFreeSpace();
        final long usedBytes = totalBytes - freeBytes;

        final double usedPercent = (double) usedBytes / (double) totalBytes * 100D;

        if (usedPercent >= threshold) {
            final String usedSpace = FormatUtils.formatDataSize(usedBytes);
            final String totalSpace = FormatUtils.formatDataSize(totalBytes);
            final String freeSpace = FormatUtils.formatDataSize(freeBytes);

            final double freePercent = (double) freeBytes / (double) totalBytes * 100D;

            /////////////////////////////////////////
            // Bulletin Board
            /////////////////////////////////////////

            final String message = String.format("%1$s exceeds configured threshold of %2$s%%, having %3$s / %4$s (%5$.2f%%) used and %6$s (%7$.2f%%) free", pathName, threshold, usedSpace, totalSpace, usedPercent, freeSpace, freePercent);
            logger.warn(message);

            /////////////////////////////////////////
            // External HTTP Service
            /////////////////////////////////////////

            if (isExternalHttpUrlEnable) {
                try {
                    Map params = new HashMap();
                    params.put("hostname", InetAddress.getLocalHost().getHostName());
                    params.put("type", "DiskUsage");
                    params.put("pathName", pathName);
                    params.put("threshold", threshold);
                    params.put("usedSpace", usedSpace);
                    params.put("totalSpace", totalSpace);
                    params.put("usedPercent", usedPercent);
                    params.put("freeSpace", freeSpace);
                    params.put("freePercent", freePercent);

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
                            logger.warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s, Status Code : %s, Response Body : %s", externalHttpUrl, response.code(), response.body().string()));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s", externalHttpUrl), e);
                }
            }
        }
    }
}
