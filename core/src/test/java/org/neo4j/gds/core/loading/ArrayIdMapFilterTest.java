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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ArrayIdMapFilterTest {
    private static final NodeLabel A = NodeLabel.of("A");
    private static final NodeLabel B = NodeLabel.of("B");
    private static final Concurrency CONCURRENCY = new Concurrency(1);

    private IdMap rootIdMap;
    private FilteredIdMap filtered;

    @BeforeEach
    void setUp() {
        var builder = ArrayIdMapBuilder.of(10);
        builder.allocate(10).insert(new long[]{100, 101, 102, 103, 104, 105, 106, 107, 108, 109});
        var lib = LabelInformationBuilders.multiLabelWithCapacity(10);
        for (long original = 100; original <= 109; original++) lib.addNodeIdToLabel(original % 2 == 0 ? A : B, original);
        this.rootIdMap = builder.build(lib, 109, CONCURRENCY);
        this.filtered = rootIdMap.withFilteredLabels(List.of(A), CONCURRENCY).orElseThrow();
    }

    @Test
    void shouldFilterAndTranslate() {
        assertThat(filtered.typeId()).isEqualTo(ArrayIdMapBuilder.ID);
        assertThat(filtered.nodeCount()).isEqualTo(5);
        assertThat(filtered.rootIdMap()).isSameAs(rootIdMap);
        assertThat(filtered.toFilteredNodeId(4)).isEqualTo(2);
        assertThat(filtered.toRootNodeId(2)).isEqualTo(4);
        assertThat(filtered.toOriginalNodeId(2)).isEqualTo(104);
        assertThat(filtered.toMappedNodeId(104)).isEqualTo(2);
        assertThat(filtered.containsRootNodeId(4)).isTrue();
        assertThat(filtered.containsRootNodeId(5)).isFalse();
        assertThat(filtered.highestOriginalId()).isEqualTo(rootIdMap.highestOriginalId());
    }

    @Test
    void shouldDelegateLabelsAndIterate() {
        assertThat(filtered.hasLabel(2, A)).isTrue();
        assertThat(filtered.nodeLabels(2)).containsExactly(A);
        assertThat(filtered.availableNodeLabels()).containsExactly(A);
        assertThat(filtered.nodeCount(A)).isEqualTo(5);
        var ids = new ArrayList<Long>();
        filtered.forEachNode(ids::add);
        assertThat(ids).containsExactly(0L, 1L, 2L, 3L, 4L);
        var viaIterator = new ArrayList<Long>();
        filtered.nodeIterator(Set.of(A)).forEachRemaining((long id) -> viaIterator.add(id));
        assertThat(viaIterator).hasSize(5);
    }

    @Test
    void shouldReturnEmptyForEmptyLabelInformation() {
        var b = ArrayIdMapBuilder.of(2);
        b.allocate(2).insert(new long[]{0, 1});
        var idMap = b.build(LabelInformationBuilders.allNodes(), 1, CONCURRENCY);
        assertThat(idMap.withFilteredLabels(List.of(NodeLabel.ALL_NODES), CONCURRENCY)).isEmpty();
    }
}
