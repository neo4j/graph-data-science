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
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PropertyMappingsTest {

    @Test
    void testFromObjectWithShorthandPropertyMapping() {
        PropertyMappings mappings = PropertyMappings.fromObject(Collections.singletonMap("foo", (Object) "bar"));
        assertEquals(mappings.numberOfMappings(), 1);

        final PropertyMapping propertyMapping = mappings.iterator().next();
        assertEquals(propertyMapping.propertyKey(), "foo");
        assertEquals(propertyMapping.neoPropertyKey(), "bar");
    }

    @Test
    void testFromObjectWithMultiplePropertieMappings() {
        PropertyMappings mappings = PropertyMappings.fromObject(MapUtil.map(
                "total_usd", MapUtil.map(
                        "property", "usd",
                        "aggregate", "MIN",
                        "defaultWeight", 42.0),
                "transaction_count", MapUtil.map(
                        "property", "usd",
                        "aggregate", "SUM"
                )
        ));
        assertEquals(mappings.numberOfMappings(), 2);

        final Iterator<PropertyMapping> mappingIterator = mappings.iterator();
        final PropertyMapping totalUsdMapping = mappingIterator.next();
        assertEquals(totalUsdMapping.propertyKey(), "total_usd");
        assertEquals(totalUsdMapping.neoPropertyKey(), "usd");
        assertEquals(totalUsdMapping.deduplicationStrategy(), DeduplicationStrategy.MIN);
        assertEquals(totalUsdMapping.defaultValue(), 42.0);

        final PropertyMapping transactionCountMapping = mappingIterator.next();
        assertEquals(transactionCountMapping.propertyKey(), "transaction_count");
        assertEquals(transactionCountMapping.neoPropertyKey(), "usd");
        assertEquals(transactionCountMapping.deduplicationStrategy(), DeduplicationStrategy.SUM);
        assertEquals(transactionCountMapping.defaultValue(), HugeGraph.NO_WEIGHT);
    }

    @Test
    void failsOnNonStringOrMapInput() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> PropertyMappings.fromObject(5));

        assertThat(ex.getMessage(), containsString("Expected String or Map for property mappings. Got Integer"));
    }

}