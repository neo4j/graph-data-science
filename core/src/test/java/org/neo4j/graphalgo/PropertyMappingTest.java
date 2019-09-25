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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.helpers.collection.MapUtil;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_PROPERTY_PARAM;

class PropertyMappingTest {

    @Test
    void testFromObjectWithShorthandPropertyMapping() {
        final PropertyMapping propertyMapping = PropertyMapping.fromObject("foo", "bar");
        assertEquals(propertyMapping.propertyKey(), "foo");
        assertEquals(propertyMapping.neoPropertyKey(), "bar");
        assertEquals(propertyMapping.deduplicationStrategy(), DeduplicationStrategy.DEFAULT);
    }

    @Test
    void testFromObjectWithMap() {
        final PropertyMapping propertyMapping = PropertyMapping.fromObject("transaction_count", MapUtil.map(
                "property", "usd",
                "aggregation", "MIN",
                "defaultValue", 42.0
        ));
        assertEquals(propertyMapping.propertyKey(), "transaction_count");
        assertEquals(propertyMapping.neoPropertyKey(), "usd");
        assertEquals(propertyMapping.deduplicationStrategy(), DeduplicationStrategy.MIN);
        assertEquals(propertyMapping.defaultValue(), 42.0);
    }

    @Test
    void failsOnWrongKeyType() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> PropertyMapping.fromObject("transaction_count", MapUtil.map(
                        "property", 42
                )));
        assertThat(ex.getMessage(), containsString("Expected the property name to be of type String, but was Integer"));
    }

    @Test
    void failsOnMissingPropertyKey() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> PropertyMapping.fromObject("transaction_count", MapUtil.map(
                        "foo", "bar"
                )));

        assertThat(
                ex.getMessage(),
                containsString(String.format(
                        "Property was not set. Missing entry with key %s",
                        RELATIONSHIP_PROPERTIES_PROPERTY_PARAM)));

    }
}