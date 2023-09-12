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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TokenMatchersTest {

    private shaded.org.apache.commons.csv.Token token;

    @BeforeEach
    public void setUp() {
        token = new shaded.org.apache.commons.csv.Token();
        token.type = shaded.org.apache.commons.csv.Token.Type.TOKEN;
        token.isReady = true;
        token.content.append("content");
    }

    @Test
    public void testHasContent() {
        Assertions.assertFalse(TokenMatchers.hasContent("This is not the token's content").matches(token));
        Assertions.assertTrue(TokenMatchers.hasContent("content").matches(token));
    }

    @Test
    public void testHasType() {
        Assertions.assertFalse(TokenMatchers.hasType(shaded.org.apache.commons.csv.Token.Type.COMMENT).matches(token));
        Assertions.assertFalse(TokenMatchers.hasType(shaded.org.apache.commons.csv.Token.Type.EOF).matches(token));
        Assertions.assertFalse(TokenMatchers.hasType(shaded.org.apache.commons.csv.Token.Type.EORECORD).matches(token));
        Assertions.assertTrue(TokenMatchers.hasType(shaded.org.apache.commons.csv.Token.Type.TOKEN).matches(token));
    }

    @Test
    public void testIsReady() {
        Assertions.assertTrue(TokenMatchers.isReady().matches(token));
        token.isReady = false;
        Assertions.assertFalse(TokenMatchers.isReady().matches(token));
    }

    @Test
    public void testMatches() {
        Assertions.assertTrue(TokenMatchers.matches(shaded.org.apache.commons.csv.Token.Type.TOKEN, "content").matches(token));
        Assertions.assertFalse(TokenMatchers.matches(shaded.org.apache.commons.csv.Token.Type.EOF, "content").matches(token));
        Assertions.assertFalse(TokenMatchers.matches(shaded.org.apache.commons.csv.Token.Type.TOKEN, "not the content").matches(token));
        Assertions.assertFalse(TokenMatchers.matches(shaded.org.apache.commons.csv.Token.Type.EORECORD, "not the content").matches(token));
    }

    @Test
    public void testToString() {
        Assertions.assertTrue(TokenMatchers.matches(shaded.org.apache.commons.csv.Token.Type.TOKEN, "content").matches(token));
        assertEquals("TOKEN", token.type.name());
        assertEquals("TOKEN [content]", token.toString());
    }
}
