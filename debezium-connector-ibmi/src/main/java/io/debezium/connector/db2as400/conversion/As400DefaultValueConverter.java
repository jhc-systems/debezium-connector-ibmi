/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2as400.conversion;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.util.Collect;

/* @See MySqlDefaultValueConverter
 *
 * based on work by @author Jiri Pechanec
 */
@Immutable
public class As400DefaultValueConverter implements DefaultValueConverter {

    private static final Logger log = LoggerFactory.getLogger(As400DefaultValueConverter.class);

    @Immutable
    private static final Set<Integer> TRIM_DATA_TYPES = Collect.unmodifiableSet(Types.TINYINT, Types.INTEGER,
            Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIME, Types.BOOLEAN, Types.BIT,
            Types.NUMERIC, Types.DECIMAL, Types.FLOAT, Types.DOUBLE, Types.REAL);

    public As400DefaultValueConverter() {
    }

    /**
     * This interface is used by a DDL parser to convert the string default value to
     * a Java type recognized by value converters for a subset of types.
     *
     * @param column                 the column definition describing the
     *                               {@code data} value; never null
     * @param defaultValueExpression the default value literal; may be null
     * @return value converted to a Java type; optional
     */
    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        try {
            Object logicalDefaultValue = convert(column, defaultValueExpression);
            if (logicalDefaultValue == null) {
                return Optional.empty();
            }
            return Optional.of(logicalDefaultValue);
        }
        catch (Exception e) {
            log.error("default conversion failed, please report", e);
            return Optional.empty();
        }
    }

    /**
     * Converts a default value from the expected format to a logical object
     * acceptable by the main JDBC converter.
     *
     * @param column column definition
     * @param value  string formatted default value
     * @return value converted to a Java type
     */
    public Object convert(Column column, String value) {
        if (value == null || "NULL".equals(value)) {
            return value;
        }

        // trim non varchar data types before converting
        if (TRIM_DATA_TYPES.contains(column.jdbcType())) {
            value = value.trim();
        }

        // boolean is also INT(1) or TINYINT(1)
        if ("TINYINT".equals(column.typeName()) || "INT".equals(column.typeName())) {
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                return convertToBoolean(value);
            }
        }
        switch (column.jdbcType()) {
            case Types.DATE: {
                if ("CURRENT_DATE".equals(value)) {
                    return null; // can't represent this as a timestamp type
                }
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return Long.valueOf(LocalDate.parse(stripQuotes(value), formatter).toEpochDay()).intValue();
            }
            case Types.TIMESTAMP: {
                if ("CURRENT_TIMESTAMP".equals(value)) {
                    return null; // can't represent this as a timestamp type
                }
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSSSSS");
                return toEpoc(LocalDateTime.parse(stripQuotes(value), formatter));
            }
            case Types.TIMESTAMP_WITH_TIMEZONE:
                throw new UnsupportedOperationException("not yet implemented to default to timestamp and timezone, value was: " + value);
            case Types.TIME:
                if ("CURRENT_TIME".equals(value)) {
                    return null;
                }
                throw new UnsupportedOperationException("not yet implemented to default to duration, value was: " + value);
            case Types.BOOLEAN:
                return convertToBoolean(value);
            case Types.BIT:
                return convertToBits(column, value);

            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return convertToDecimal(column, value);

            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return convertToDouble(value);
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
                return stripQuotes(value);
            case Types.INTEGER:
            case Types.SMALLINT:
                return Integer.parseInt(value);
        }
        return value;
    }

    private String stripQuotes(String value) {
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private Long toEpoc(LocalDateTime date) {
        ZoneId zoneId = ZoneId.systemDefault();
        return date.atZone(zoneId).toEpochSecond();
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DOUBLE}.
     *
     * @param value the string object to be converted into a {@link Types#DOUBLE}
     *              type;
     * @return the converted value;
     */
    private Object convertToDouble(String value) {
        return Double.parseDouble(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DECIMAL}.
     *
     * @param column the column definition describing the {@code data} value; never
     *               null
     * @param value  the string object to be converted into a {@link Types#DECIMAL}
     *               type;
     * @return the converted value;
     */
    private Object convertToDecimal(Column column, String value) {
        return column.scale().isPresent() ? new BigDecimal(value).setScale(column.scale().get(), RoundingMode.HALF_UP)
                : new BigDecimal(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#BIT}.
     *
     * @param column the column definition describing the {@code data} value; never
     *               null
     * @param value  the string object to be converted into a {@link Types#BIT}
     *               type;
     * @return the converted value;
     */
    private Object convertToBits(Column column, String value) {
        if (column.length() > 1) {
            return convertToBits(value);
        }
        return convertToBit(value);
    }

    private Object convertToBit(String value) {
        try {
            return Short.parseShort(value) != 0;
        }
        catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

    private Object convertToBits(String value) {
        int nums = value.length() / Byte.SIZE + (value.length() % Byte.SIZE == 0 ? 0 : 1);
        byte[] bytes = new byte[nums];
        for (int i = 0; i < nums; i++) {
            int s = value.length() - Byte.SIZE < 0 ? 0 : value.length() - Byte.SIZE;
            int e = value.length();
            bytes[nums - i - 1] = (byte) Integer.parseInt(value.substring(s, e), 2);
            value = value.substring(0, s);
        }
        return bytes;
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#BOOLEAN}.
     * 
     * @param value the string object to be converted into a {@link Types#BOOLEAN}
     *              type;
     *
     * @return the converted value;
     */
    private Object convertToBoolean(String value) {
        try {
            return Integer.parseInt(value) != 0;
        }
        catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

    private DateTimeFormatter timestampFormat(int length) {
        final DateTimeFormatterBuilder dtf = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").optionalStart()
                .appendLiteral(" ").append(DateTimeFormatter.ISO_LOCAL_TIME).optionalEnd()
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0).parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);
        if (length > 0) {
            dtf.appendFraction(ChronoField.MICRO_OF_SECOND, 0, length, true);
        }
        return dtf.toFormatter();
    }

}
