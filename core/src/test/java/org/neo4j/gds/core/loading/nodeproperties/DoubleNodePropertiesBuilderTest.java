/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.core.loading.nodeproperties;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.ToMappedNodeId;
import org.neo4j.gds.config.ConcurrencyConfig;

import static org.assertj.core.api.Assertions.assertThat;

class DoubleNodePropertiesBuilderTest {

    @Test
    void identityFastPathPreservesValuesMaxAndCount() {
        int nodeCount = 1000;
        var builder = new DoubleNodePropertiesBuilder(DefaultValue.of(10.0), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);

        double expectedMax = Double.NEGATIVE_INFINITY;
        for (long i = 0; i < nodeCount; i++) {
            double value = i * 7.0;
            builder.set(i, value);
            expectedMax = Math.max(expectedMax, value);
        }

        var properties = builder.build(nodeCount, ToMappedNodeId.IDENTITY, nodeCount - 1);

        for (long i = 0; i < nodeCount; i++) {
            assertThat(properties.doubleValue(i)).as("value at %d", i).isEqualTo(i * 7.0);
        }
        assertThat(properties.nodeCount()).isEqualTo(nodeCount);
        assertThat(properties.getMaxDoublePropertyValue()).hasValue(expectedMax);
    }

    @Test
    void identityFastPathTreatsExplicitDefaultAsAbsent() {
        // The fast-path reuses the source array (the copy path would drop defaults), but
        // hasValue/contains is value-based (value != defaultValue), so an explicitly-set
        // default is observably absent on both paths — the fast-path leaks nothing.
        var builder = new DoubleNodePropertiesBuilder(DefaultValue.of(10.0), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        builder.set(0, 10.0);   // explicit default
        builder.set(1, 42.0);

        var properties = builder.build(2, ToMappedNodeId.IDENTITY, 1);

        assertThat(properties.hasValue(0)).isFalse();
        assertThat(properties.doubleValue(0)).isEqualTo(10.0);
        assertThat(properties.hasValue(1)).isTrue();
        assertThat(properties.doubleValue(1)).isEqualTo(42.0);
    }

    @Test
    void identityFastPathOnEmptyBuilderHasNoMax() {
        var builder = new DoubleNodePropertiesBuilder(DefaultValue.of(10.0), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        var properties = builder.build(0, ToMappedNodeId.IDENTITY, -1);
        assertThat(properties.getMaxDoublePropertyValue()).isEmpty();
    }
}
