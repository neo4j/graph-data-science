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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.api.IdMap.NOT_FOUND;

class HighLimitIdMapTest {

    private HighLimitIdMap idMap;

    @BeforeEach
    void setup() {
        var builder = new LazyIdMapBuilder(4, true, false, PropertyState.PERSISTENT);
        builder.addNode(1000, NodeLabelTokens.ofStrings("A"));
        builder.addNode(2000, NodeLabelTokens.ofStrings("B"));
        builder.addNode(3000, NodeLabelTokens.ofStrings("C"));

        this.idMap = builder.build().idMap();
    }

    @Test
    void testToRootNodeId() {
        idMap.forEachNode(nodeId -> {
            assertThat(idMap.toRootNodeId(nodeId)).isEqualTo(nodeId);
            return true;
        });
    }

    @Test
    void shouldHandleUnmappedIds() {
        var intermediateIdMapBuilder = ShardedLongLongMap.builder(new Concurrency(1));
        intermediateIdMapBuilder.addNode(0);
        var intermediateIdMap = intermediateIdMapBuilder.build(0);
        var internalIdMap = new DirectIdMap(1);

        var highLimitIdMap = new HighLimitIdMap(intermediateIdMap, internalIdMap);

        assertThat(highLimitIdMap.toMappedNodeId(1337)).isEqualTo(NOT_FOUND);
        assertThat(highLimitIdMap.containsOriginalId(1337)).isFalse();
    }

    @Test
    void shouldReturnCorrectTypeIds() {
        long[] nodes = LongStream.range(0, 42).toArray();
        var concurrency = new Concurrency(1);
        var builder = HighLimitIdMapBuilder.of(concurrency, ArrayIdMapBuilder.of(nodes.length));
        builder.allocate(nodes.length).insert(nodes);
        var idMap = builder.build(LabelInformationBuilders.allNodes(), nodes.length - 1, concurrency);

        assertThat(idMap.typeId()).contains(HighLimitIdMapBuilder.ID).contains(ArrayIdMapBuilder.ID);
        assertThat(HighLimitIdMap.innerTypeId(idMap.typeId()).get()).isEqualTo(ArrayIdMapBuilder.ID);
    }

    @Test
    void shouldReturnCorrectInnerTypeId() {
        assertThat(HighLimitIdMap.innerTypeId(HighLimitIdMapBuilder.ID)).isEqualTo(Optional.empty());
        assertThat(HighLimitIdMap.innerTypeId(HighLimitIdMapBuilder.ID + "-foobar")).isEqualTo(Optional.of("foobar"));
        assertThat(HighLimitIdMap.innerTypeId(HighLimitIdMapBuilder.ID + "-" + HighLimitIdMapBuilder.ID)).isEqualTo(Optional.empty());
        assertThat(HighLimitIdMap.innerTypeId("foobar")).isEqualTo(Optional.empty());
        assertThat(HighLimitIdMap.innerTypeId(HighLimitIdMapBuilder.ID + "-")).isEqualTo(Optional.empty());
    }

    @Test
    void shouldIdentifyHighLimitIdMaps() {
        assertThat(HighLimitIdMap.isHighLimitIdMap(HighLimitIdMapBuilder.ID)).isTrue();
        assertThat(HighLimitIdMap.isHighLimitIdMap("foobar")).isFalse();
    }

    @Nested
    class FilteredHighLimitIdMap {

        @Test
        void testToRootNodeId() {
            var concurrency = new Concurrency(1);
            var filteredIdMapA = idMap.withFilteredLabels(List.of(NodeLabel.of("A")), concurrency).get();
            long expectedA = idMap.toMappedNodeId(1000);
            assertThat(filteredIdMapA.toRootNodeId(0)).isEqualTo(expectedA);

            var filteredIdMapB = idMap.withFilteredLabels(List.of(NodeLabel.of("B")), concurrency).get();
            long expectedB = idMap.toMappedNodeId(2000);
            assertThat(filteredIdMapB.toRootNodeId(0)).isEqualTo(expectedB);


            var filteredIdMapC = idMap.withFilteredLabels(List.of(NodeLabel.of("C")), concurrency).get();
            long expectedC = idMap.toMappedNodeId(3000);
            assertThat(filteredIdMapC.toRootNodeId(0)).isEqualTo(expectedC);
        }
    }

}
