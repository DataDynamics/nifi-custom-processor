package io.datadynamics.nifi.kudu.converter;

import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.apache.nifi.util.StringUtils;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;

/**
 * Convert Object to java.sql.Timestamp using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
public class ObjectTimestampFieldConverter implements FieldConverter<Object, Timestamp> {

    /**
     * Convert Object field to java.sql.Timestamp using optional format supported in DateTimeFormatter
     *
     * @param field            Field can be null or a supported input type
     * @param pattern          Format pattern optional for parsing
     * @param name             Field name for tracking
     * @param addHour          Hours to Add
     * @param timestampPattern Timestamp Pattern
     * @return Timestamp or null when input field is null or empty string
     * @throws IllegalTypeConversionException Thrown on parsing failures or unsupported types of input fields
     */
    @Override
    public Timestamp convertField(final Object field,
                                  final Optional<String> pattern,
                                  final String name,
                                  final int addHour,
                                  String timestampPattern) {
        if (field == null) {
            return null;
        }

        if (field instanceof Timestamp) {
            return (Timestamp) field;
        }

        if (field instanceof Date) {
            final Date date = (Date) field;
            return new Timestamp(date.getTime());
        }

        if (field instanceof Number) {
            final Number number = (Number) field;
            return new Timestamp(number.longValue());
        }

        if (field instanceof String) {
            final String string = field.toString().trim();
            if (string.isEmpty()) {
                return null;
            }

            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampPattern);
            try {
                // 날짜 패턴으로 데이터를 파싱하고 AddHour를 추가한다.
                final LocalDateTime localDateTime = LocalDateTime.parse(string, formatter);
                if (addHour != 0) {
                    LocalDateTime plus = localDateTime.plus(Duration.ofHours(addHour));
                    return Timestamp.valueOf(plus);
                } else {
                    return Timestamp.valueOf(localDateTime);
                }
            } catch (final DateTimeParseException e1) {
                // 파싱 에러가 발생하면 숫자로 다시 파싱을 시도한다.
                try {
                    final long number = Long.parseLong(string);
                    return new Timestamp(number);
                } catch (final NumberFormatException e2) {
                    final String message = String.format("Convert Field Name [%s] Value [%s] to Timestamp Long parsing failed: %s", name, field, e2.getMessage());
                    throw new IllegalTypeConversionException(message);
                }
            }
        }

        final String message = String.format("Convert Field Name [%s] Value [%s] Class [%s] to Timestamp not supported", name, field, field.getClass());
        throw new IllegalTypeConversionException(message);
    }
}