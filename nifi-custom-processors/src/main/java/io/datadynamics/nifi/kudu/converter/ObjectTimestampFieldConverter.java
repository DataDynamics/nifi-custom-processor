package io.datadynamics.nifi.kudu.converter;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;

public class ObjectTimestampFieldConverter implements FieldConverter<Object, Timestamp> {

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
            if (addHour != 0) {
                long number = ((Timestamp) field).getTime();
                Date newDate = DateUtils.addMilliseconds(new Date(number), addHour * (60 * 1000) * 60);
                return new Timestamp(newDate.getTime());
            } else {
                return (Timestamp) field;
            }
        }

        if (field instanceof Date) {
            if (addHour != 0) {
                final Date date = (Date) field;
                Date newDate = DateUtils.addMilliseconds(date, addHour * (60 * 1000) * 60);
                return new Timestamp(newDate.getTime());
            } else {
                final Date date = (Date) field;
                return new Timestamp(date.getTime());
            }
        }

        if (field instanceof Number) {
            if (addHour != 0) {
                final Number number = (Number) field;
                Date newDate = DateUtils.addMilliseconds(new Date(number.longValue()), addHour * (60 * 1000) * 60);
                return new Timestamp(newDate.getTime());
            } else {
                final Number number = (Number) field;
                return new Timestamp(number.longValue());
            }
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
                    if (addHour != 0) {
                        Date newDate = DateUtils.addMilliseconds(new Date(number), addHour * (60 * 1000) * 60);
                        return new Timestamp(newDate.getTime());
                    } else {
                        return new Timestamp(number);
                    }
                } catch (final NumberFormatException e2) {
                    final String message = String.format("필드명 [%s] 값 [%s]을 Timestamp로 변환할 수 없습니다: %s", name, field, e2.getMessage());
                    throw new IllegalTypeConversionException(message);
                }
            }
        }

        final String message = String.format("필드명 [%s] 값 [%s] 클래스 [%s]는 Timestamp로 변환을 지원하지 않습니다.", name, field, field.getClass());
        throw new IllegalTypeConversionException(message);
    }
}