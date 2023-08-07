package io.datadynamics.nifi.kudu;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

public class TimestampUtils {

    private static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
        protected DateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getDefault());
            return sdf;
        }
    };

    public static Timestamp parseTimestampMicro(String value) {
        return parseTimestamp(value, "yyyy-MM-dd HH:mm:ss.SSSSSS");
    }

    public static Timestamp parseTimestampMillis(String value) {
        return parseTimestamp(value, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    public static Timestamp parseTimestamp(String value, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
        return Timestamp.valueOf(dateTime);
    }


    public static long timestampToMicros(Timestamp timestamp) {
        long millis = timestamp.getTime() * 1000L;
        long micros = (long) timestamp.getNanos() % 1000000L / 1000L;
        return micros >= 0L ? millis + micros : millis + 1000000L + micros;
    }

    public static Timestamp microsToTimestamp(long micros) {
        long millis = micros / 1000L;
        long nanos = micros % 1000000L * 1000L;
        if (nanos < 0L) {
            --millis;
            nanos += 1000000000L;
        }

        Timestamp timestamp = new Timestamp(millis);
        timestamp.setNanos((int) nanos);
        return timestamp;
    }

    public static String timestampToString(Timestamp timestamp) {
        long micros = timestampToMicros(timestamp);
        return timestampToString(micros);
    }

    public static String timestampToString(long micros) {
        long tsMillis = micros / 1000L;
        long tsMicros = micros % 1000000L;
        String tsStr = ((DateFormat) DATE_FORMAT.get()).format(new Date(tsMillis));
        return String.format("%s.%06dZ", tsStr, tsMicros);
    }
}
