/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shaded.org.apache.commons.csv;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import shaded.org.apache.commons.csv.CSVFormat.Builder;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static shaded.org.apache.commons.csv.CSVFormat.RFC4180;
import static shaded.org.apache.commons.csv.Constants.*;

/**
 * Tests {@link shaded.org.apache.commons.csv.CSVFormat}.
 */
public class CSVFormatTest {

    private static void assertNotEquals(final Object right, final Object left) {
        Assertions.assertNotEquals(right, left);
        Assertions.assertNotEquals(left, right);
    }

    private static shaded.org.apache.commons.csv.CSVFormat copy(final shaded.org.apache.commons.csv.CSVFormat format) {
        return format.builder().setDelimiter(format.getDelimiter()).build();
    }

    private void assertNotEquals(final String name, final String type, final Object left, final Object right) {
        if (left.equals(right) || right.equals(left)) {
            fail("Objects must not compare equal for " + name + "(" + type + ")");
        }
        if (left.hashCode() == right.hashCode()) {
            fail("Hash code should not be equal for " + name + "(" + type + ")");
        }
    }

    @Test
    public void testDelimiterEmptyStringThrowsException1() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setDelimiter("").build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDelimiterSameAsCommentStartThrowsException_Deprecated() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter('!').withCommentMarker('!'));
    }

    @Test
    public void testDelimiterSameAsCommentStartThrowsException1() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setDelimiter('!').setCommentMarker('!').build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDelimiterSameAsEscapeThrowsException_Deprecated() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter('!').withEscape('!'));
    }

    @Test
    public void testDelimiterSameAsEscapeThrowsException1() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setDelimiter('!').setEscape('!').build());
    }

    @Test
    public void testDelimiterSameAsRecordSeparatorThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.newFormat(CR));
    }

    @Test
    public void testDuplicateHeaderElements() {
        final String[] header = {"A", "A"};
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setHeader(header).build();
        assertEquals(2, format.getHeader().length);
        assertArrayEquals(header, format.getHeader());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDuplicateHeaderElements_Deprecated() {
        final String[] header = {"A", "A"};
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader(header);
        assertEquals(2, format.getHeader().length);
        assertArrayEquals(header, format.getHeader());
    }

    @Test
    public void testDuplicateHeaderElementsFalse() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setAllowDuplicateHeaderNames(false).setHeader("A", "A").build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDuplicateHeaderElementsFalse_Deprecated() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withAllowDuplicateHeaderNames(false).withHeader("A", "A"));
    }

    @Test
    public void testDuplicateHeaderElementsTrue() {
        shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setAllowDuplicateHeaderNames(true).setHeader("A", "A").build();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDuplicateHeaderElementsTrue_Deprecated() {
        shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withAllowDuplicateHeaderNames(true).withHeader("A", "A");
    }

    @Test
    public void testDuplicateHeaderElementsTrueContainsEmpty1() {
        shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setAllowDuplicateHeaderNames(false).setHeader("A", "", "B", "").build();
    }

    @Test
    public void testDuplicateHeaderElementsTrueContainsEmpty2() {
        shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_EMPTY).setHeader("A", "", "B", "").build();
    }

    @Test
    public void testDuplicateHeaderElementsTrueContainsEmpty3() {
        shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setAllowDuplicateHeaderNames(false).setAllowMissingColumnNames(true).setHeader("A", "", "B", "").build();
    }

    @Test
    public void testEquals() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.DEFAULT;
        final shaded.org.apache.commons.csv.CSVFormat left = copy(right);

        Assertions.assertNotEquals(null, right);
        Assertions.assertNotEquals("A String Instance", right);

        assertEquals(right, right);
        assertEquals(right, left);
        assertEquals(left, right);

        assertEquals(right.hashCode(), right.hashCode());
        assertEquals(right.hashCode(), left.hashCode());
    }

    @Test
    public void testEqualsCommentStart() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setQuote('"').setCommentMarker('#').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setCommentMarker('!').build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsCommentStart_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withQuote('"').withCommentMarker('#').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        final shaded.org.apache.commons.csv.CSVFormat left = right.withCommentMarker('!');

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsDelimiter() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('!');
        final shaded.org.apache.commons.csv.CSVFormat left = shaded.org.apache.commons.csv.CSVFormat.newFormat('?');

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsEscape() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setQuote('"').setCommentMarker('#').setEscape('+').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setEscape('!').build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsEscape_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withQuote('"').withCommentMarker('#').withEscape('+').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        final shaded.org.apache.commons.csv.CSVFormat left = right.withEscape('!');

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsHeader() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setRecordSeparator(CR).setCommentMarker('#').setEscape('+').setHeader("One", "Two", "Three")
                .setIgnoreEmptyLines(true).setIgnoreSurroundingSpaces(true).setQuote('"').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setHeader("Three", "Two", "One").build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsHeader_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withRecordSeparator(CR).withCommentMarker('#').withEscape('+').withHeader("One", "Two", "Three")
                .withIgnoreEmptyLines().withIgnoreSurroundingSpaces().withQuote('"').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        final shaded.org.apache.commons.csv.CSVFormat left = right.withHeader("Three", "Two", "One");

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsIgnoreEmptyLines() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setCommentMarker('#').setEscape('+').setIgnoreEmptyLines(true)
                .setIgnoreSurroundingSpaces(true).setQuote('"').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setIgnoreEmptyLines(false).build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsIgnoreEmptyLines_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withCommentMarker('#').withEscape('+').withIgnoreEmptyLines().withIgnoreSurroundingSpaces()
                .withQuote('"').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        final shaded.org.apache.commons.csv.CSVFormat left = right.withIgnoreEmptyLines(false);

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsIgnoreSurroundingSpaces() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setCommentMarker('#').setEscape('+').setIgnoreSurroundingSpaces(true).setQuote('"')
                .setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setIgnoreSurroundingSpaces(false).build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsIgnoreSurroundingSpaces_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withCommentMarker('#').withEscape('+').withIgnoreSurroundingSpaces().withQuote('"')
                .withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        final shaded.org.apache.commons.csv.CSVFormat left = right.withIgnoreSurroundingSpaces(false);

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsLeftNoQuoteRightQuote() {
        final shaded.org.apache.commons.csv.CSVFormat left = shaded.org.apache.commons.csv.CSVFormat.newFormat(',').builder().setQuote(null).build();
        final shaded.org.apache.commons.csv.CSVFormat right = left.builder().setQuote('#').build();

        assertNotEquals(left, right);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsLeftNoQuoteRightQuote_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat left = shaded.org.apache.commons.csv.CSVFormat.newFormat(',').withQuote(null);
        final shaded.org.apache.commons.csv.CSVFormat right = left.withQuote('#');

        assertNotEquals(left, right);
    }

    @Test
    public void testEqualsNoQuotes() {
        final shaded.org.apache.commons.csv.CSVFormat left = shaded.org.apache.commons.csv.CSVFormat.newFormat(',').builder().setQuote(null).build();
        final shaded.org.apache.commons.csv.CSVFormat right = left.builder().setQuote(null).build();

        assertEquals(left, right);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsNoQuotes_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat left = shaded.org.apache.commons.csv.CSVFormat.newFormat(',').withQuote(null);
        final shaded.org.apache.commons.csv.CSVFormat right = left.withQuote(null);

        assertEquals(left, right);
    }

    @Test
    public void testEqualsNullString() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setRecordSeparator(CR).setCommentMarker('#').setEscape('+').setIgnoreEmptyLines(true)
                .setIgnoreSurroundingSpaces(true).setQuote('"').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).setNullString("null").build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setNullString("---").build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsNullString_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withRecordSeparator(CR).withCommentMarker('#').withEscape('+').withIgnoreEmptyLines()
                .withIgnoreSurroundingSpaces().withQuote('"').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).withNullString("null");
        final shaded.org.apache.commons.csv.CSVFormat left = right.withNullString("---");

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsOne() {

        final shaded.org.apache.commons.csv.CSVFormat csvFormatOne = shaded.org.apache.commons.csv.CSVFormat.INFORMIX_UNLOAD;
        final shaded.org.apache.commons.csv.CSVFormat csvFormatTwo = shaded.org.apache.commons.csv.CSVFormat.MYSQL;

        assertEquals('\\', (char) csvFormatOne.getEscapeCharacter());
        assertNull(csvFormatOne.getQuoteMode());

        assertTrue(csvFormatOne.getIgnoreEmptyLines());
        assertFalse(csvFormatOne.getSkipHeaderRecord());

        assertFalse(csvFormatOne.getIgnoreHeaderCase());
        assertNull(csvFormatOne.getCommentMarker());

        assertFalse(csvFormatOne.isCommentMarkerSet());
        assertTrue(csvFormatOne.isQuoteCharacterSet());

        assertEquals('|', csvFormatOne.getDelimiter());
        assertFalse(csvFormatOne.getAllowMissingColumnNames());

        assertTrue(csvFormatOne.isEscapeCharacterSet());
        assertEquals("\n", csvFormatOne.getRecordSeparator());

        assertEquals('\"', (char) csvFormatOne.getQuoteCharacter());
        assertFalse(csvFormatOne.getTrailingDelimiter());

        assertFalse(csvFormatOne.getTrim());
        assertFalse(csvFormatOne.isNullStringSet());

        assertNull(csvFormatOne.getNullString());
        assertFalse(csvFormatOne.getIgnoreSurroundingSpaces());

        assertTrue(csvFormatTwo.isEscapeCharacterSet());
        assertNull(csvFormatTwo.getQuoteCharacter());

        assertFalse(csvFormatTwo.getAllowMissingColumnNames());
        assertEquals(shaded.org.apache.commons.csv.QuoteMode.ALL_NON_NULL, csvFormatTwo.getQuoteMode());

        assertEquals('\t', csvFormatTwo.getDelimiter());
        assertEquals("\n", csvFormatTwo.getRecordSeparator());

        assertFalse(csvFormatTwo.isQuoteCharacterSet());
        assertTrue(csvFormatTwo.isNullStringSet());

        assertEquals('\\', (char) csvFormatTwo.getEscapeCharacter());
        assertFalse(csvFormatTwo.getIgnoreHeaderCase());

        assertFalse(csvFormatTwo.getTrim());
        assertFalse(csvFormatTwo.getIgnoreEmptyLines());

        assertEquals("\\N", csvFormatTwo.getNullString());
        assertFalse(csvFormatTwo.getIgnoreSurroundingSpaces());

        assertFalse(csvFormatTwo.getTrailingDelimiter());
        assertFalse(csvFormatTwo.getSkipHeaderRecord());

        assertNull(csvFormatTwo.getCommentMarker());
        assertFalse(csvFormatTwo.isCommentMarkerSet());

        assertNotSame(csvFormatTwo, csvFormatOne);
        Assertions.assertNotEquals(csvFormatTwo, csvFormatOne);

        assertEquals('\\', (char) csvFormatOne.getEscapeCharacter());
        assertNull(csvFormatOne.getQuoteMode());

        assertTrue(csvFormatOne.getIgnoreEmptyLines());
        assertFalse(csvFormatOne.getSkipHeaderRecord());

        assertFalse(csvFormatOne.getIgnoreHeaderCase());
        assertNull(csvFormatOne.getCommentMarker());

        assertFalse(csvFormatOne.isCommentMarkerSet());
        assertTrue(csvFormatOne.isQuoteCharacterSet());

        assertEquals('|', csvFormatOne.getDelimiter());
        assertFalse(csvFormatOne.getAllowMissingColumnNames());

        assertTrue(csvFormatOne.isEscapeCharacterSet());
        assertEquals("\n", csvFormatOne.getRecordSeparator());

        assertEquals('\"', (char) csvFormatOne.getQuoteCharacter());
        assertFalse(csvFormatOne.getTrailingDelimiter());

        assertFalse(csvFormatOne.getTrim());
        assertFalse(csvFormatOne.isNullStringSet());

        assertNull(csvFormatOne.getNullString());
        assertFalse(csvFormatOne.getIgnoreSurroundingSpaces());

        assertTrue(csvFormatTwo.isEscapeCharacterSet());
        assertNull(csvFormatTwo.getQuoteCharacter());

        assertFalse(csvFormatTwo.getAllowMissingColumnNames());
        assertEquals(shaded.org.apache.commons.csv.QuoteMode.ALL_NON_NULL, csvFormatTwo.getQuoteMode());

        assertEquals('\t', csvFormatTwo.getDelimiter());
        assertEquals("\n", csvFormatTwo.getRecordSeparator());

        assertFalse(csvFormatTwo.isQuoteCharacterSet());
        assertTrue(csvFormatTwo.isNullStringSet());

        assertEquals('\\', (char) csvFormatTwo.getEscapeCharacter());
        assertFalse(csvFormatTwo.getIgnoreHeaderCase());

        assertFalse(csvFormatTwo.getTrim());
        assertFalse(csvFormatTwo.getIgnoreEmptyLines());

        assertEquals("\\N", csvFormatTwo.getNullString());
        assertFalse(csvFormatTwo.getIgnoreSurroundingSpaces());

        assertFalse(csvFormatTwo.getTrailingDelimiter());
        assertFalse(csvFormatTwo.getSkipHeaderRecord());

        assertNull(csvFormatTwo.getCommentMarker());
        assertFalse(csvFormatTwo.isCommentMarkerSet());

        assertNotSame(csvFormatOne, csvFormatTwo);
        assertNotSame(csvFormatTwo, csvFormatOne);

        Assertions.assertNotEquals(csvFormatOne, csvFormatTwo);
        Assertions.assertNotEquals(csvFormatTwo, csvFormatOne);

        Assertions.assertNotEquals(csvFormatTwo, csvFormatOne);

    }

    @Test
    public void testEqualsQuoteChar() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setQuote('"').build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setQuote('!').build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsQuoteChar_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withQuote('"');
        final shaded.org.apache.commons.csv.CSVFormat left = right.withQuote('!');

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsQuotePolicy() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setQuote('"').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.MINIMAL).build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsQuotePolicy_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withQuote('"').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        final shaded.org.apache.commons.csv.CSVFormat left = right.withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.MINIMAL);

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsRecordSeparator() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setRecordSeparator(CR).setCommentMarker('#').setEscape('+').setIgnoreEmptyLines(true)
                .setIgnoreSurroundingSpaces(true).setQuote('"').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setRecordSeparator(LF).build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsRecordSeparator_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withRecordSeparator(CR).withCommentMarker('#').withEscape('+').withIgnoreEmptyLines()
                .withIgnoreSurroundingSpaces().withQuote('"').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        final shaded.org.apache.commons.csv.CSVFormat left = right.withRecordSeparator(LF);

        assertNotEquals(right, left);
    }

    public void testEqualsSkipHeaderRecord() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').builder().setRecordSeparator(CR).setCommentMarker('#').setEscape('+').setIgnoreEmptyLines(true)
                .setIgnoreSurroundingSpaces(true).setQuote('"').setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).setNullString("null").setSkipHeaderRecord(true).build();
        final shaded.org.apache.commons.csv.CSVFormat left = right.builder().setSkipHeaderRecord(false).build();

        assertNotEquals(right, left);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsSkipHeaderRecord_Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat right = shaded.org.apache.commons.csv.CSVFormat.newFormat('\'').withRecordSeparator(CR).withCommentMarker('#').withEscape('+').withIgnoreEmptyLines()
                .withIgnoreSurroundingSpaces().withQuote('"').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL).withNullString("null").withSkipHeaderRecord();
        final shaded.org.apache.commons.csv.CSVFormat left = right.withSkipHeaderRecord(false);

        assertNotEquals(right, left);
    }

    @Test
    public void testEqualsWithNull() {

        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.POSTGRESQL_TEXT;

        assertEquals('\\', (char) csvFormat.getEscapeCharacter());
        assertFalse(csvFormat.getIgnoreSurroundingSpaces());

        assertFalse(csvFormat.getTrailingDelimiter());
        assertFalse(csvFormat.getTrim());

        assertFalse(csvFormat.isQuoteCharacterSet());
        assertEquals("\\N", csvFormat.getNullString());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertTrue(csvFormat.isEscapeCharacterSet());

        assertFalse(csvFormat.isCommentMarkerSet());
        assertNull(csvFormat.getCommentMarker());

        assertFalse(csvFormat.getAllowMissingColumnNames());
        assertEquals(shaded.org.apache.commons.csv.QuoteMode.ALL_NON_NULL, csvFormat.getQuoteMode());

        assertEquals('\t', csvFormat.getDelimiter());
        assertFalse(csvFormat.getSkipHeaderRecord());

        assertEquals("\n", csvFormat.getRecordSeparator());
        assertFalse(csvFormat.getIgnoreEmptyLines());

        assertNull(csvFormat.getQuoteCharacter());
        assertTrue(csvFormat.isNullStringSet());

        assertEquals('\\', (char) csvFormat.getEscapeCharacter());
        assertFalse(csvFormat.getIgnoreSurroundingSpaces());

        assertFalse(csvFormat.getTrailingDelimiter());
        assertFalse(csvFormat.getTrim());

        assertFalse(csvFormat.isQuoteCharacterSet());
        assertEquals("\\N", csvFormat.getNullString());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertTrue(csvFormat.isEscapeCharacterSet());

        assertFalse(csvFormat.isCommentMarkerSet());
        assertNull(csvFormat.getCommentMarker());

        assertFalse(csvFormat.getAllowMissingColumnNames());
        assertEquals(shaded.org.apache.commons.csv.QuoteMode.ALL_NON_NULL, csvFormat.getQuoteMode());

        assertEquals('\t', csvFormat.getDelimiter());
        assertFalse(csvFormat.getSkipHeaderRecord());

        assertEquals("\n", csvFormat.getRecordSeparator());
        assertFalse(csvFormat.getIgnoreEmptyLines());

        assertNull(csvFormat.getQuoteCharacter());
        assertTrue(csvFormat.isNullStringSet());

        Assertions.assertNotEquals(null, csvFormat);

    }

    @Test
    public void testEscapeSameAsCommentStartThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setEscape('!').setCommentMarker('!').build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEscapeSameAsCommentStartThrowsException_Deprecated() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withEscape('!').withCommentMarker('!'));
    }

    @Test
    public void testEscapeSameAsCommentStartThrowsExceptionForWrapperType() {
        // Cannot assume that callers won't use different Character objects
        assertThrows(IllegalArgumentException.class,
                () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setEscape(Character.valueOf('!')).setCommentMarker(Character.valueOf('!')).build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEscapeSameAsCommentStartThrowsExceptionForWrapperType_Deprecated() {
        // Cannot assume that callers won't use different Character objects
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withEscape(Character.valueOf('!')).withCommentMarker(Character.valueOf('!')));
    }

    @Test
    public void testFormat() {
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT;

        assertEquals("", format.format());
        assertEquals("a,b,c", format.format("a", "b", "c"));
        assertEquals("\"x,y\",z", format.format("x,y", "z"));
    }

    @Test // I assume this to be a defect.
    public void testFormatThrowsNullPointerException() {

        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.MYSQL;

        final NullPointerException e = assertThrows(NullPointerException.class, () -> csvFormat.format((Object[]) null));
        assertEquals(Objects.class.getName(), e.getStackTrace()[0].getClassName());
    }

    @Test
    public void testFormatToString() {
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180.withEscape('?').withDelimiter(',').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.MINIMAL).withRecordSeparator(CRLF).withQuote('"')
                .withNullString("").withIgnoreHeaderCase(true).withHeaderComments("This is HeaderComments").withHeader("col1", "col2", "col3");
        assertEquals(
                "Delimiter=<,> Escape=<?> QuoteChar=<\"> QuoteMode=<MINIMAL> NullString=<> RecordSeparator=<" + CRLF
                        + "> IgnoreHeaderCase:ignored SkipHeaderRecord:false HeaderComments:[This is HeaderComments] Header:[col1, col2, col3]",
                format.toString());
    }

    @Test
    public void testGetAllowDuplicateHeaderNames() {
        final Builder builder = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder();
        assertTrue(builder.build().getAllowDuplicateHeaderNames());
        assertTrue(builder.setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_ALL).build().getAllowDuplicateHeaderNames());
        assertFalse(builder.setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_EMPTY).build().getAllowDuplicateHeaderNames());
        assertFalse(builder.setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.DISALLOW).build().getAllowDuplicateHeaderNames());
    }

    @Test
    public void testGetDuplicateHeaderMode() {
        final Builder builder = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder();

        assertEquals(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_ALL, builder.build().getDuplicateHeaderMode());
        assertEquals(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_ALL, builder.setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_ALL).build().getDuplicateHeaderMode());
        assertEquals(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_EMPTY, builder.setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_EMPTY).build().getDuplicateHeaderMode());
        assertEquals(shaded.org.apache.commons.csv.DuplicateHeaderMode.DISALLOW, builder.setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.DISALLOW).build().getDuplicateHeaderMode());
    }

    @Test
    public void testGetHeader() {
        final String[] header = {"one", "two", "three"};
        final shaded.org.apache.commons.csv.CSVFormat formatWithHeader = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader(header);
        // getHeader() makes a copy of the header array.
        final String[] headerCopy = formatWithHeader.getHeader();
        headerCopy[0] = "A";
        headerCopy[1] = "B";
        headerCopy[2] = "C";
        assertFalse(Arrays.equals(formatWithHeader.getHeader(), headerCopy));
        assertNotSame(formatWithHeader.getHeader(), headerCopy);
    }

    @Test
    public void testHashCodeAndWithIgnoreHeaderCase() {

        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.INFORMIX_UNLOAD_CSV;
        final shaded.org.apache.commons.csv.CSVFormat csvFormatTwo = csvFormat.withIgnoreHeaderCase();
        csvFormatTwo.hashCode();

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertTrue(csvFormatTwo.getIgnoreHeaderCase()); // now different
        assertFalse(csvFormatTwo.getTrailingDelimiter());

        Assertions.assertNotEquals(csvFormatTwo, csvFormat); // CSV-244 - should not be equal
        assertFalse(csvFormatTwo.getAllowMissingColumnNames());

        assertFalse(csvFormatTwo.getTrim());

    }

    @Test
    public void testJiraCsv236() {
        shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setAllowDuplicateHeaderNames(true).setHeader("CC", "VV", "VV").build();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testJiraCsv236__Deprecated() {
        shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withAllowDuplicateHeaderNames().withHeader("CC", "VV", "VV");
    }

    @Test
    public void testNewFormat() {

        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.newFormat('X');

        assertFalse(csvFormat.getSkipHeaderRecord());
        assertFalse(csvFormat.isEscapeCharacterSet());

        assertNull(csvFormat.getRecordSeparator());
        assertNull(csvFormat.getQuoteMode());

        assertNull(csvFormat.getCommentMarker());
        assertFalse(csvFormat.getIgnoreHeaderCase());

        assertFalse(csvFormat.getAllowMissingColumnNames());
        assertFalse(csvFormat.getTrim());

        assertFalse(csvFormat.isNullStringSet());
        assertNull(csvFormat.getEscapeCharacter());

        assertFalse(csvFormat.getIgnoreSurroundingSpaces());
        assertFalse(csvFormat.getTrailingDelimiter());

        assertEquals('X', csvFormat.getDelimiter());
        assertNull(csvFormat.getNullString());

        assertFalse(csvFormat.isQuoteCharacterSet());
        assertFalse(csvFormat.isCommentMarkerSet());

        assertNull(csvFormat.getQuoteCharacter());
        assertFalse(csvFormat.getIgnoreEmptyLines());

        assertFalse(csvFormat.getSkipHeaderRecord());
        assertFalse(csvFormat.isEscapeCharacterSet());

        assertNull(csvFormat.getRecordSeparator());
        assertNull(csvFormat.getQuoteMode());

        assertNull(csvFormat.getCommentMarker());
        assertFalse(csvFormat.getIgnoreHeaderCase());

        assertFalse(csvFormat.getAllowMissingColumnNames());
        assertFalse(csvFormat.getTrim());

        assertFalse(csvFormat.isNullStringSet());
        assertNull(csvFormat.getEscapeCharacter());

        assertFalse(csvFormat.getIgnoreSurroundingSpaces());
        assertFalse(csvFormat.getTrailingDelimiter());

        assertEquals('X', csvFormat.getDelimiter());
        assertNull(csvFormat.getNullString());

        assertFalse(csvFormat.isQuoteCharacterSet());
        assertFalse(csvFormat.isCommentMarkerSet());

        assertNull(csvFormat.getQuoteCharacter());
        assertFalse(csvFormat.getIgnoreEmptyLines());

    }

    @Test
    public void testNullRecordSeparatorCsv106() {
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.newFormat(';').builder().setSkipHeaderRecord(true).setHeader("H1", "H2").build();
        final String formatStr = format.format("A", "B");
        assertNotNull(formatStr);
        assertFalse(formatStr.endsWith("null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testNullRecordSeparatorCsv106__Deprecated() {
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.newFormat(';').withSkipHeaderRecord().withHeader("H1", "H2");
        final String formatStr = format.format("A", "B");
        assertNotNull(formatStr);
        assertFalse(formatStr.endsWith("null"));
    }

    @Test
    public void testPrintRecord() throws IOException {
        final Appendable out = new StringBuilder();
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180;
        format.printRecord(out, "a", "b", "c");
        assertEquals("a,b,c" + format.getRecordSeparator(), out.toString());
    }

    @Test
    public void testPrintRecordEmpty() throws IOException {
        final Appendable out = new StringBuilder();
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180;
        format.printRecord(out);
        assertEquals(format.getRecordSeparator(), out.toString());
    }

    @Test
    public void testPrintWithEscapesEndWithCRLF() throws IOException {
        final Reader in = new StringReader("x,y,x\r\na,?b,c\r\n");
        final Appendable out = new StringBuilder();
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180.withEscape('?').withDelimiter(',').withQuote(null).withRecordSeparator(CRLF);
        format.print(in, out, true);
        assertEquals("x?,y?,x?r?na?,??b?,c?r?n", out.toString());
    }

    @Test
    public void testPrintWithEscapesEndWithoutCRLF() throws IOException {
        final Reader in = new StringReader("x,y,x");
        final Appendable out = new StringBuilder();
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180.withEscape('?').withDelimiter(',').withQuote(null).withRecordSeparator(CRLF);
        format.print(in, out, true);
        assertEquals("x?,y?,x", out.toString());
    }

    @Test
    public void testPrintWithoutQuotes() throws IOException {
        final Reader in = new StringReader("");
        final Appendable out = new StringBuilder();
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180.withDelimiter(',').withQuote('"').withEscape('?').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.NON_NUMERIC);
        format.print(in, out, true);
        assertEquals("\"\"", out.toString());
    }

    @Test
    public void testPrintWithQuoteModeIsNONE() throws IOException {
        final Reader in = new StringReader("a,b,c");
        final Appendable out = new StringBuilder();
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180.withDelimiter(',').withQuote('"').withEscape('?').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.NONE);
        format.print(in, out, true);
        assertEquals("a?,b?,c", out.toString());
    }

    @Test
    public void testPrintWithQuotes() throws IOException {
        final Reader in = new StringReader("\"a,b,c\r\nx,y,z");
        final Appendable out = new StringBuilder();
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.RFC4180.withDelimiter(',').withQuote('"').withEscape('?').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.NON_NUMERIC);
        format.print(in, out, true);
        assertEquals("\"\"\"a,b,c\r\nx,y,z\"", out.toString());
    }

    @Test
    public void testQuoteCharSameAsCommentStartThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setQuote('!').setCommentMarker('!').build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testQuoteCharSameAsCommentStartThrowsException_Deprecated() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote('!').withCommentMarker('!'));
    }

    @Test
    public void testQuoteCharSameAsCommentStartThrowsExceptionForWrapperType() {
        // Cannot assume that callers won't use different Character objects
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setQuote(Character.valueOf('!')).setCommentMarker('!').build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testQuoteCharSameAsCommentStartThrowsExceptionForWrapperType_Deprecated() {
        // Cannot assume that callers won't use different Character objects
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote(Character.valueOf('!')).withCommentMarker('!'));
    }

    @Test
    public void testQuoteCharSameAsDelimiterThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setQuote('!').setDelimiter('!').build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testQuoteCharSameAsDelimiterThrowsException_Deprecated() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote('!').withDelimiter('!'));
    }

    @Test
    public void testQuotePolicyNoneWithoutEscapeThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.newFormat('!').builder().setQuoteMode(shaded.org.apache.commons.csv.QuoteMode.NONE).build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testQuotePolicyNoneWithoutEscapeThrowsException_Deprecated() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.newFormat('!').withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.NONE));
    }

    @Test
    public void testRFC4180() {
        assertNull(RFC4180.getCommentMarker());
        assertEquals(',', RFC4180.getDelimiter());
        assertNull(RFC4180.getEscapeCharacter());
        assertFalse(RFC4180.getIgnoreEmptyLines());
        assertEquals(Character.valueOf('"'), RFC4180.getQuoteCharacter());
        assertNull(RFC4180.getQuoteMode());
        assertEquals("\r\n", RFC4180.getRecordSeparator());
    }

    @SuppressWarnings("boxing") // no need to worry about boxing here
    @Test
    public void testSerialization() throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (final ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(shaded.org.apache.commons.csv.CSVFormat.DEFAULT);
            oos.flush();
        }

        final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
        final shaded.org.apache.commons.csv.CSVFormat format = (shaded.org.apache.commons.csv.CSVFormat) in.readObject();

        assertNotNull(format);
        assertEquals(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.getDelimiter(), format.getDelimiter(), "delimiter");
        assertEquals(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.getQuoteCharacter(), format.getQuoteCharacter(), "encapsulator");
        assertEquals(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.getCommentMarker(), format.getCommentMarker(), "comment start");
        assertEquals(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.getRecordSeparator(), format.getRecordSeparator(), "record separator");
        assertEquals(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.getEscapeCharacter(), format.getEscapeCharacter(), "escape");
        assertEquals(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.getIgnoreSurroundingSpaces(), format.getIgnoreSurroundingSpaces(), "trim");
        assertEquals(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.getIgnoreEmptyLines(), format.getIgnoreEmptyLines(), "empty lines");
    }

    @Test
    public void testToString() {

        final String string = shaded.org.apache.commons.csv.CSVFormat.INFORMIX_UNLOAD.toString();

        assertEquals("Delimiter=<|> Escape=<\\> QuoteChar=<\"> RecordSeparator=<\n> EmptyLines:ignored SkipHeaderRecord:false", string);

    }

    @Test
    public void testToStringAndWithCommentMarkerTakingCharacter() {

        final shaded.org.apache.commons.csv.CSVFormat.Predefined csvFormat_Predefined = shaded.org.apache.commons.csv.CSVFormat.Predefined.Default;
        final shaded.org.apache.commons.csv.CSVFormat csvFormat = csvFormat_Predefined.getFormat();

        assertNull(csvFormat.getEscapeCharacter());
        assertTrue(csvFormat.isQuoteCharacterSet());

        assertFalse(csvFormat.getTrim());
        assertFalse(csvFormat.getIgnoreSurroundingSpaces());

        assertFalse(csvFormat.getTrailingDelimiter());
        assertEquals(',', csvFormat.getDelimiter());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertEquals("\r\n", csvFormat.getRecordSeparator());

        assertFalse(csvFormat.isCommentMarkerSet());
        assertNull(csvFormat.getCommentMarker());

        assertFalse(csvFormat.isNullStringSet());
        assertFalse(csvFormat.getAllowMissingColumnNames());

        assertFalse(csvFormat.isEscapeCharacterSet());
        assertFalse(csvFormat.getSkipHeaderRecord());

        assertNull(csvFormat.getNullString());
        assertNull(csvFormat.getQuoteMode());

        assertTrue(csvFormat.getIgnoreEmptyLines());
        assertEquals('\"', (char) csvFormat.getQuoteCharacter());

        final Character character = Character.valueOf('n');

        final shaded.org.apache.commons.csv.CSVFormat csvFormatTwo = csvFormat.withCommentMarker(character);

        assertNull(csvFormat.getEscapeCharacter());
        assertTrue(csvFormat.isQuoteCharacterSet());

        assertFalse(csvFormat.getTrim());
        assertFalse(csvFormat.getIgnoreSurroundingSpaces());

        assertFalse(csvFormat.getTrailingDelimiter());
        assertEquals(',', csvFormat.getDelimiter());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertEquals("\r\n", csvFormat.getRecordSeparator());

        assertFalse(csvFormat.isCommentMarkerSet());
        assertNull(csvFormat.getCommentMarker());

        assertFalse(csvFormat.isNullStringSet());
        assertFalse(csvFormat.getAllowMissingColumnNames());

        assertFalse(csvFormat.isEscapeCharacterSet());
        assertFalse(csvFormat.getSkipHeaderRecord());

        assertNull(csvFormat.getNullString());
        assertNull(csvFormat.getQuoteMode());

        assertTrue(csvFormat.getIgnoreEmptyLines());
        assertEquals('\"', (char) csvFormat.getQuoteCharacter());

        assertFalse(csvFormatTwo.isNullStringSet());
        assertFalse(csvFormatTwo.getAllowMissingColumnNames());

        assertEquals('\"', (char) csvFormatTwo.getQuoteCharacter());
        assertNull(csvFormatTwo.getNullString());

        assertEquals(',', csvFormatTwo.getDelimiter());
        assertFalse(csvFormatTwo.getTrailingDelimiter());

        assertTrue(csvFormatTwo.isCommentMarkerSet());
        assertFalse(csvFormatTwo.getIgnoreHeaderCase());

        assertFalse(csvFormatTwo.getTrim());
        assertNull(csvFormatTwo.getEscapeCharacter());

        assertTrue(csvFormatTwo.isQuoteCharacterSet());
        assertFalse(csvFormatTwo.getIgnoreSurroundingSpaces());

        assertEquals("\r\n", csvFormatTwo.getRecordSeparator());
        assertNull(csvFormatTwo.getQuoteMode());

        assertEquals('n', (char) csvFormatTwo.getCommentMarker());
        assertFalse(csvFormatTwo.getSkipHeaderRecord());

        assertFalse(csvFormatTwo.isEscapeCharacterSet());
        assertTrue(csvFormatTwo.getIgnoreEmptyLines());

        assertNotSame(csvFormat, csvFormatTwo);
        assertNotSame(csvFormatTwo, csvFormat);

        Assertions.assertNotEquals(csvFormatTwo, csvFormat);

        assertNull(csvFormat.getEscapeCharacter());
        assertTrue(csvFormat.isQuoteCharacterSet());

        assertFalse(csvFormat.getTrim());
        assertFalse(csvFormat.getIgnoreSurroundingSpaces());

        assertFalse(csvFormat.getTrailingDelimiter());
        assertEquals(',', csvFormat.getDelimiter());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertEquals("\r\n", csvFormat.getRecordSeparator());

        assertFalse(csvFormat.isCommentMarkerSet());
        assertNull(csvFormat.getCommentMarker());

        assertFalse(csvFormat.isNullStringSet());
        assertFalse(csvFormat.getAllowMissingColumnNames());

        assertFalse(csvFormat.isEscapeCharacterSet());
        assertFalse(csvFormat.getSkipHeaderRecord());

        assertNull(csvFormat.getNullString());
        assertNull(csvFormat.getQuoteMode());

        assertTrue(csvFormat.getIgnoreEmptyLines());
        assertEquals('\"', (char) csvFormat.getQuoteCharacter());

        assertFalse(csvFormatTwo.isNullStringSet());
        assertFalse(csvFormatTwo.getAllowMissingColumnNames());

        assertEquals('\"', (char) csvFormatTwo.getQuoteCharacter());
        assertNull(csvFormatTwo.getNullString());

        assertEquals(',', csvFormatTwo.getDelimiter());
        assertFalse(csvFormatTwo.getTrailingDelimiter());

        assertTrue(csvFormatTwo.isCommentMarkerSet());
        assertFalse(csvFormatTwo.getIgnoreHeaderCase());

        assertFalse(csvFormatTwo.getTrim());
        assertNull(csvFormatTwo.getEscapeCharacter());

        assertTrue(csvFormatTwo.isQuoteCharacterSet());
        assertFalse(csvFormatTwo.getIgnoreSurroundingSpaces());

        assertEquals("\r\n", csvFormatTwo.getRecordSeparator());
        assertNull(csvFormatTwo.getQuoteMode());

        assertEquals('n', (char) csvFormatTwo.getCommentMarker());
        assertFalse(csvFormatTwo.getSkipHeaderRecord());

        assertFalse(csvFormatTwo.isEscapeCharacterSet());
        assertTrue(csvFormatTwo.getIgnoreEmptyLines());

        assertNotSame(csvFormat, csvFormatTwo);
        assertNotSame(csvFormatTwo, csvFormat);

        Assertions.assertNotEquals(csvFormat, csvFormatTwo);

        Assertions.assertNotEquals(csvFormatTwo, csvFormat);
        assertEquals("Delimiter=<,> QuoteChar=<\"> CommentStart=<n> " + "RecordSeparator=<\r\n> EmptyLines:ignored SkipHeaderRecord:false",
                csvFormatTwo.toString());

    }

    @Test
    public void testTrim() throws IOException {
        final shaded.org.apache.commons.csv.CSVFormat formatWithTrim = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(',').withTrim().withQuote(null).withRecordSeparator(CRLF);

        CharSequence in = "a,b,c";
        final StringBuilder out = new StringBuilder();
        formatWithTrim.print(in, out, true);
        assertEquals("a,b,c", out.toString());

        in = new StringBuilder(" x,y,z");
        out.setLength(0);
        formatWithTrim.print(in, out, true);
        assertEquals("x,y,z", out.toString());

        in = new StringBuilder();
        out.setLength(0);
        formatWithTrim.print(in, out, true);
        assertEquals("", out.toString());

        in = new StringBuilder("header\r\n");
        out.setLength(0);
        formatWithTrim.print(in, out, true);
        assertEquals("header", out.toString());
    }

    @Test
    public void testWithCommentStart() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithCommentStart = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withCommentMarker('#');
        assertEquals(Character.valueOf('#'), formatWithCommentStart.getCommentMarker());
    }

    @Test
    public void testWithCommentStartCRThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withCommentMarker(CR));
    }

    @Test
    public void testWithDelimiter() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithDelimiter = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter('!');
        assertEquals('!', formatWithDelimiter.getDelimiter());
    }

    @Test
    public void testWithDelimiterLFThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(LF));
    }

    @Test
    public void testWithEmptyDuplicates() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithEmptyDuplicates = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.builder().setDuplicateHeaderMode(shaded.org.apache.commons.csv.DuplicateHeaderMode.ALLOW_EMPTY).build();

        assertEquals(DuplicateHeaderMode.ALLOW_EMPTY, formatWithEmptyDuplicates.getDuplicateHeaderMode());
        assertFalse(formatWithEmptyDuplicates.getAllowDuplicateHeaderNames());
    }

    @Test
    public void testWithEmptyEnum() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithHeader = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader(EmptyEnum.class);
        assertEquals(0, formatWithHeader.getHeader().length);
    }

    @Test
    public void testWithEscape() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithEscape = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withEscape('&');
        assertEquals(Character.valueOf('&'), formatWithEscape.getEscapeCharacter());
    }

    @Test
    public void testWithEscapeCRThrowsExceptions() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withEscape(CR));
    }

    @Test
    public void testWithFirstRecordAsHeader() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithFirstRecordAsHeader = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withFirstRecordAsHeader();
        assertTrue(formatWithFirstRecordAsHeader.getSkipHeaderRecord());
        assertEquals(0, formatWithFirstRecordAsHeader.getHeader().length);
    }

    @Test
    public void testWithHeader() {
        final String[] header = {"one", "two", "three"};
        // withHeader() makes a copy of the header array.
        final shaded.org.apache.commons.csv.CSVFormat formatWithHeader = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader(header);
        assertArrayEquals(header, formatWithHeader.getHeader());
        assertNotSame(header, formatWithHeader.getHeader());
    }

    @Test
    public void testWithHeaderComments() {

        final shaded.org.apache.commons.csv.CSVFormat csvFormat = shaded.org.apache.commons.csv.CSVFormat.DEFAULT;

        assertEquals('\"', (char) csvFormat.getQuoteCharacter());
        assertFalse(csvFormat.isCommentMarkerSet());

        assertFalse(csvFormat.isEscapeCharacterSet());
        assertTrue(csvFormat.isQuoteCharacterSet());

        assertFalse(csvFormat.getSkipHeaderRecord());
        assertNull(csvFormat.getQuoteMode());

        assertEquals(',', csvFormat.getDelimiter());
        assertTrue(csvFormat.getIgnoreEmptyLines());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertNull(csvFormat.getCommentMarker());

        assertEquals("\r\n", csvFormat.getRecordSeparator());
        assertFalse(csvFormat.getTrailingDelimiter());

        assertFalse(csvFormat.getAllowMissingColumnNames());
        assertFalse(csvFormat.getTrim());

        assertFalse(csvFormat.isNullStringSet());
        assertNull(csvFormat.getNullString());

        assertFalse(csvFormat.getIgnoreSurroundingSpaces());
        assertNull(csvFormat.getEscapeCharacter());

        final Object[] objectArray = new Object[8];
        final shaded.org.apache.commons.csv.CSVFormat csvFormatTwo = csvFormat.withHeaderComments(objectArray);

        assertEquals('\"', (char) csvFormat.getQuoteCharacter());
        assertFalse(csvFormat.isCommentMarkerSet());

        assertFalse(csvFormat.isEscapeCharacterSet());
        assertTrue(csvFormat.isQuoteCharacterSet());

        assertFalse(csvFormat.getSkipHeaderRecord());
        assertNull(csvFormat.getQuoteMode());

        assertEquals(',', csvFormat.getDelimiter());
        assertTrue(csvFormat.getIgnoreEmptyLines());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertNull(csvFormat.getCommentMarker());

        assertEquals("\r\n", csvFormat.getRecordSeparator());
        assertFalse(csvFormat.getTrailingDelimiter());

        assertFalse(csvFormat.getAllowMissingColumnNames());
        assertFalse(csvFormat.getTrim());

        assertFalse(csvFormat.isNullStringSet());
        assertNull(csvFormat.getNullString());

        assertFalse(csvFormat.getIgnoreSurroundingSpaces());
        assertNull(csvFormat.getEscapeCharacter());

        assertFalse(csvFormatTwo.getIgnoreHeaderCase());
        assertNull(csvFormatTwo.getQuoteMode());

        assertTrue(csvFormatTwo.getIgnoreEmptyLines());
        assertFalse(csvFormatTwo.getIgnoreSurroundingSpaces());

        assertNull(csvFormatTwo.getEscapeCharacter());
        assertFalse(csvFormatTwo.getTrim());

        assertFalse(csvFormatTwo.isEscapeCharacterSet());
        assertTrue(csvFormatTwo.isQuoteCharacterSet());

        assertFalse(csvFormatTwo.getSkipHeaderRecord());
        assertEquals('\"', (char) csvFormatTwo.getQuoteCharacter());

        assertFalse(csvFormatTwo.getAllowMissingColumnNames());
        assertNull(csvFormatTwo.getNullString());

        assertFalse(csvFormatTwo.isNullStringSet());
        assertFalse(csvFormatTwo.getTrailingDelimiter());

        assertEquals("\r\n", csvFormatTwo.getRecordSeparator());
        assertEquals(',', csvFormatTwo.getDelimiter());

        assertNull(csvFormatTwo.getCommentMarker());
        assertFalse(csvFormatTwo.isCommentMarkerSet());

        assertNotSame(csvFormat, csvFormatTwo);
        assertNotSame(csvFormatTwo, csvFormat);

        Assertions.assertNotEquals(csvFormatTwo, csvFormat); // CSV-244 - should not be equal

        final String string = csvFormatTwo.format(objectArray);

        assertEquals('\"', (char) csvFormat.getQuoteCharacter());
        assertFalse(csvFormat.isCommentMarkerSet());

        assertFalse(csvFormat.isEscapeCharacterSet());
        assertTrue(csvFormat.isQuoteCharacterSet());

        assertFalse(csvFormat.getSkipHeaderRecord());
        assertNull(csvFormat.getQuoteMode());

        assertEquals(',', csvFormat.getDelimiter());
        assertTrue(csvFormat.getIgnoreEmptyLines());

        assertFalse(csvFormat.getIgnoreHeaderCase());
        assertNull(csvFormat.getCommentMarker());

        assertEquals("\r\n", csvFormat.getRecordSeparator());
        assertFalse(csvFormat.getTrailingDelimiter());

        assertFalse(csvFormat.getAllowMissingColumnNames());
        assertFalse(csvFormat.getTrim());

        assertFalse(csvFormat.isNullStringSet());
        assertNull(csvFormat.getNullString());

        assertFalse(csvFormat.getIgnoreSurroundingSpaces());
        assertNull(csvFormat.getEscapeCharacter());

        assertFalse(csvFormatTwo.getIgnoreHeaderCase());
        assertNull(csvFormatTwo.getQuoteMode());

        assertTrue(csvFormatTwo.getIgnoreEmptyLines());
        assertFalse(csvFormatTwo.getIgnoreSurroundingSpaces());

        assertNull(csvFormatTwo.getEscapeCharacter());
        assertFalse(csvFormatTwo.getTrim());

        assertFalse(csvFormatTwo.isEscapeCharacterSet());
        assertTrue(csvFormatTwo.isQuoteCharacterSet());

        assertFalse(csvFormatTwo.getSkipHeaderRecord());
        assertEquals('\"', (char) csvFormatTwo.getQuoteCharacter());

        assertFalse(csvFormatTwo.getAllowMissingColumnNames());
        assertNull(csvFormatTwo.getNullString());

        assertFalse(csvFormatTwo.isNullStringSet());
        assertFalse(csvFormatTwo.getTrailingDelimiter());

        assertEquals("\r\n", csvFormatTwo.getRecordSeparator());
        assertEquals(',', csvFormatTwo.getDelimiter());

        assertNull(csvFormatTwo.getCommentMarker());
        assertFalse(csvFormatTwo.isCommentMarkerSet());

        assertNotSame(csvFormat, csvFormatTwo);
        assertNotSame(csvFormatTwo, csvFormat);

        assertNotNull(string);
        Assertions.assertNotEquals(csvFormat, csvFormatTwo); // CSV-244 - should not be equal

        Assertions.assertNotEquals(csvFormatTwo, csvFormat); // CSV-244 - should not be equal
        assertEquals(",,,,,,,", string);

    }

    @Test
    public void testWithHeaderEnum() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithHeader = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withHeader(Header.class);
        assertArrayEquals(new String[]{"Name", "Email", "Phone"}, formatWithHeader.getHeader());
    }

    @Test
    public void testWithHeaderEnumNull() {
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT;
        final Class<Enum<?>> simpleName = null;
        format.withHeader(simpleName);
    }

    @Test
    public void testWithHeaderResultSetNull() throws SQLException {
        final shaded.org.apache.commons.csv.CSVFormat format = shaded.org.apache.commons.csv.CSVFormat.DEFAULT;
        final ResultSet resultSet = null;
        format.withHeader(resultSet);
    }

    @Test
    public void testWithIgnoreEmptyLines() {
        assertFalse(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withIgnoreEmptyLines(false).getIgnoreEmptyLines());
        assertTrue(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withIgnoreEmptyLines().getIgnoreEmptyLines());
    }

    @Test
    public void testWithIgnoreSurround() {
        assertFalse(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withIgnoreSurroundingSpaces(false).getIgnoreSurroundingSpaces());
        assertTrue(shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withIgnoreSurroundingSpaces().getIgnoreSurroundingSpaces());
    }

    @Test
    public void testWithNullString() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithNullString = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withNullString("null");
        assertEquals("null", formatWithNullString.getNullString());
    }

    @Test
    public void testWithQuoteChar() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithQuoteChar = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote('"');
        assertEquals(Character.valueOf('"'), formatWithQuoteChar.getQuoteCharacter());
    }

    @Test
    public void testWithQuoteLFThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuote(LF));
    }

    @Test
    public void testWithQuotePolicy() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithQuotePolicy = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withQuoteMode(shaded.org.apache.commons.csv.QuoteMode.ALL);
        assertEquals(QuoteMode.ALL, formatWithQuotePolicy.getQuoteMode());
    }

    @Test
    public void testWithRecordSeparatorCR() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithRecordSeparator = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withRecordSeparator(CR);
        assertEquals(String.valueOf(CR), formatWithRecordSeparator.getRecordSeparator());
    }

    @Test
    public void testWithRecordSeparatorCRLF() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithRecordSeparator = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withRecordSeparator(CRLF);
        assertEquals(CRLF, formatWithRecordSeparator.getRecordSeparator());
    }

    @Test
    public void testWithRecordSeparatorLF() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithRecordSeparator = shaded.org.apache.commons.csv.CSVFormat.DEFAULT.withRecordSeparator(LF);
        assertEquals(String.valueOf(LF), formatWithRecordSeparator.getRecordSeparator());
    }

    @Test
    public void testWithSystemRecordSeparator() {
        final shaded.org.apache.commons.csv.CSVFormat formatWithRecordSeparator = CSVFormat.DEFAULT.withSystemRecordSeparator();
        assertEquals(System.lineSeparator(), formatWithRecordSeparator.getRecordSeparator());
    }

    public enum EmptyEnum {
        // empty enum.
    }

    public enum Header {
        Name, Email, Phone
    }
}
