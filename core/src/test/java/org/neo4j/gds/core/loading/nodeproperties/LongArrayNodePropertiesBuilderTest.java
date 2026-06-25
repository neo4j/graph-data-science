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

class LongArrayNodePropertiesBuilderTest {

    @Test
    void identityFastPathPreservesValuesAndCount() {
        int nodeCount = 1000;
        var builder = new LongArrayNodePropertiesBuilder(DefaultValue.of(new long[]{-1L}), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);

        for (long i = 0; i < nodeCount; i++) {
            builder.set(i, new long[]{i, i * 2});
        }

        var properties = builder.build(nodeCount, ToMappedNodeId.IDENTITY, nodeCount - 1);

        for (long i = 0; i < nodeCount; i++) {
            assertThat(properties.longArrayValue(i)).as("value at %d", i).containsExactly(i, i * 2);
        }
        assertThat(properties.nodeCount()).isEqualTo(nodeCount);
    }

    @Test
    void identityFastPathTreatsExplicitDefaultAsAbsent() {
        // The fast-path reuses the source array (the copy path would drop defaults), but
        // hasValue/contains is value-based (value != defaultValue), so an explicitly-set
        // default is observably absent on both paths — the fast-path leaks nothing.
        var builder = new LongArrayNodePropertiesBuilder(DefaultValue.of(new long[]{-1L}), ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY);
        builder.set(0, new long[]{-1L});   // explicit default
        builder.set(1, new long[]{7L});

        var properties = builder.build(2, ToMappedNodeId.IDENTITY, 1);

        assertThat(properties.hasValue(0)).isFalse();
        assertThat(properties.longArrayValue(0)).containsExactly(-1L);
        assertThat(properties.hasValue(1)).isTrue();
        assertThat(properties.longArrayValue(1)).containsExactly(7L);
    }
}
