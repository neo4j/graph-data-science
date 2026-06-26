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
import org.neo4j.gds.api.NodeIdMapper;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.loading.construction.GraphFactory;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class LongNodePropertiesBuilderTest {

    @Test
    void singleLabelAssignmentWithNonDirectMapping() {
        int nodeCount = 10;
        var defaultValue = DefaultValue.of(10L);

        var originalIds = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            originalIds[i] = i * 42L;
        }

        var idMapBuilder = GraphFactory
            .initNodesBuilder()
            .nodeCount(originalIds.length)
            .maxOriginalId(originalIds[nodeCount - 1])
            .build();
        Arrays.stream(originalIds).forEach(idMapBuilder::addNode);

        var idMap =  idMapBuilder.build().idMap();

        var builder = LongNodePropertiesBuilder.of(
            defaultValue,
            ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY
        );

        for (int i = 0; i < nodeCount; i++) {
            builder.set(originalIds[i], i * 1337L);
        }

        var longNodeProperties = builder.build(10, idMap::toMappedNodeId, idMap.highestOriginalId());

        for (int i = 0; i < nodeCount; i++) {
            assertThat(longNodeProperties.longValue(i)).isEqualTo(i * 1337L);
        }
    }

    @Test
    void identityFastPathPreservesValuesMaxAndCount() {
        int nodeCount = 1000;
        var builder = LongNodePropertiesBuilder.of(DefaultValue.of(10L), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);

        long expectedMax = Long.MIN_VALUE;
        for (long i = 0; i < nodeCount; i++) {
            long value = i * 7L;
            builder.set(i, value);
            expectedMax = Math.max(expectedMax, value);
        }

        var properties = builder.build(nodeCount, NodeIdMapper.IDENTITY, nodeCount - 1);

        for (long i = 0; i < nodeCount; i++) {
            assertThat(properties.longValue(i)).as("value at %d", i).isEqualTo(i * 7L);
        }
        assertThat(properties.nodeCount()).isEqualTo(nodeCount);
        assertThat(properties.getMaxLongPropertyValue()).hasValue(expectedMax);
    }

    @Test
    void identityFastPathTreatsExplicitDefaultAsAbsent() {
        // The fast-path reuses the source array (the copy path would drop defaults), but
        // hasValue/contains is value-based (value != defaultValue), so an explicitly-set
        // default is observably absent on both paths — the fast-path leaks nothing.
        var builder = LongNodePropertiesBuilder.of(DefaultValue.of(10L), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        builder.set(0, 10L);   // explicit default
        builder.set(1, 42L);

        var properties = builder.build(2, NodeIdMapper.IDENTITY, 1);

        assertThat(properties.hasValue(0)).isFalse();
        assertThat(properties.longValue(0)).isEqualTo(10L);
        assertThat(properties.hasValue(1)).isTrue();
        assertThat(properties.longValue(1)).isEqualTo(42L);
    }

    @Test
    void identityFastPathOnEmptyBuilderHasNoMax() {
        var builder = LongNodePropertiesBuilder.of(DefaultValue.of(10L), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        var properties = builder.build(0, NodeIdMapper.IDENTITY, -1);
        assertThat(properties.getMaxLongPropertyValue()).isEmpty();
    }
}
