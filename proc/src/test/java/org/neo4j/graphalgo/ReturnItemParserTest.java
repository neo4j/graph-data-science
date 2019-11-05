/*
 * Copyright (c) 2017-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.graphalgo;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LowerChars;
import net.jqwik.api.constraints.NumericChars;
import net.jqwik.api.constraints.StringLength;
import net.jqwik.api.constraints.UpperChars;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReturnItemParserTest {
    private static final Set<String> COMMUNITY_COUNT_FIELD = Collections.singleton("communityCount");
    private static final Set<String> SET_COUNT_FIELD = Collections.singleton("setCount");
    private static final Set<String> HISTOGRAM_FIELDS = new HashSet<>(Arrays.asList("p01", "p75", "p100"));

    @Test
    void testParseCommunityCount() {
        assertTrue(BaseProc.ReturnItemParser.computeCommunityCount(COMMUNITY_COUNT_FIELD));
    }

    @Test
    void testParseSetCount() {
        assertTrue(BaseProc.ReturnItemParser.computeCommunityCount(SET_COUNT_FIELD));
    }

    @Test
    void testParsePercentiles() {
        assertTrue(BaseProc.ReturnItemParser.computeHistogram(HISTOGRAM_FIELDS));
    }

    @Property
    void testNegativeFields(@ForAll @StringLength(min = 1, max = 15) @UpperChars @LowerChars @NumericChars String field) {
        assertFalse(BaseProc.ReturnItemParser.computeCommunityCount(Collections.singleton(field)));
        assertFalse(BaseProc.ReturnItemParser.computeHistogram(Collections.singleton(field)));
    }
}