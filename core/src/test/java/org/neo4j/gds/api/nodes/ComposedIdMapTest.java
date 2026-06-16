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
package org.neo4j.gds.api.nodes;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.LabelInformationBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ComposedIdMapTest {

    private static final NodeLabel A = NodeLabel.of("A");
    private static final NodeLabel B = NodeLabel.of("B");
    private static final Concurrency CONCURRENCY = new Concurrency(1);

    private static final class ListTranslator implements FilterableNodeTranslator {
        // original id = mapped id + offset
        private final long[] mappedToOriginal;

        ListTranslator(long[] mappedToOriginal) {
            this.mappedToOriginal = mappedToOriginal;
        }

        @Override
        public String typeId() {
            return "list";
        }

        @Override
        public long toMappedNodeId(long originalNodeId) {
            for (int i = 0; i < mappedToOriginal.length; i++) {
                if (mappedToOriginal[i] == originalNodeId) {
                    return i;
                }
            }
            return NOT_FOUND;
        }

        @Override
        public long toOriginalNodeId(long mappedNodeId) {return mappedToOriginal[(int) mappedNodeId];}

        @Override
        public boolean containsOriginalId(long originalNodeId) {return toMappedNodeId(originalNodeId) != NOT_FOUND;}

        @Override
        public long nodeCount() {return mappedToOriginal.length;}

        @Override
        public long highestOriginalId() {return mappedToOriginal[mappedToOriginal.length - 1];}

        @Override
        public NodeTranslator filteredNodeTranslator(BitSet unionBitSet, Concurrency concurrency) {
            var selected = new ArrayList<Long>();
            var bitIterator = unionBitSet.iterator();

            long index;
            while ((index = bitIterator.nextSetBit()) != BitSetIterator.NO_MORE) {
                selected.add(index);
            }

            return new ListTranslator(selected.stream().mapToLong(Long::longValue).toArray());
        }
    }

    @Test
    void shouldDelegateTranslationAndLabels() {
        var idMap = idMap(10, 100);
        assertThat(idMap.typeId()).isEqualTo("list");
        assertThat(idMap.nodeCount()).isEqualTo(10);
        assertThat(idMap.toMappedNodeId(104)).isEqualTo(4);
        assertThat(idMap.toOriginalNodeId(4)).isEqualTo(104);
        assertThat(idMap.containsOriginalId(104)).isTrue();
        assertThat(idMap.highestOriginalId()).isEqualTo(109);
        assertThat(idMap.rootNodeCount()).hasValue(10);
        assertThat(idMap.rootIdMap()).isSameAs(idMap);
        assertThat(idMap.toRootNodeId(7)).isEqualTo(7);
        assertThat(idMap.availableNodeLabels()).containsExactlyInAnyOrder(A, B);
        assertThat(idMap.hasLabel(4, A)).isTrue();
        assertThat(idMap.nodeCount(A)).isEqualTo(5);
    }

    @Test
    void shouldIterateAllNodesAndByLabel() {
        var idMap = idMap(10, 100);
        var seen = new ArrayList<Long>();
        idMap.forEachNode(seen::add);
        assertThat(seen).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);

        var onlyA = new ArrayList<Long>();
        idMap.nodeIterator(Set.of(A)).forEachRemaining((long id) -> onlyA.add(id));
        assertThat(onlyA).containsExactlyInAnyOrder(0L, 2L, 4L, 6L, 8L);

        assertThat(idMap.batchIterables(3)).hasSize(4);
    }

    @Test
    void shouldUpgradeSingleLabelOnMutation() {
        var translator = new ListTranslator(new long[]{100, 101, 102});
        var idMap = ComposedIdMap.of(
            translator,
            LabelInformationBuilders.singleLabel(A).build(3, translator::toMappedNodeId)
        );
        idMap.addNodeLabel(B);
        idMap.addNodeIdToLabel(1, B);
        assertThat(idMap.availableNodeLabels()).contains(A, B);
        assertThat(idMap.hasLabel(1, B)).isTrue();
    }

    @Test
    void shouldComposeFilteredView() {
        var filtered = idMap(10, 100).withFilteredLabels(List.of(A), CONCURRENCY).orElseThrow();
        assertThat(filtered.nodeCount()).isEqualTo(5);
        assertThat(filtered.toFilteredNodeId(4)).isEqualTo(2);   // root mapped 4 -> filtered 2
        assertThat(filtered.toRootNodeId(2)).isEqualTo(4);
        assertThat(filtered.toOriginalNodeId(2)).isEqualTo(104);
        assertThat(filtered.toMappedNodeId(104)).isEqualTo(2);
        assertThat(filtered.containsRootNodeId(4)).isTrue();
        assertThat(filtered.containsRootNodeId(5)).isFalse();
        assertThat(filtered.typeId()).isEqualTo("list");
        assertThat(filtered.hasLabel(2, A)).isTrue();            // filtered 2 -> root 4 -> A
        assertThat(filtered.availableNodeLabels()).containsExactly(A);
    }

    @Test
    void shouldThrowWhenTranslatorNotFilterable() {
        var translator = new NodeTranslator() {
            @Override
            public String typeId() {return "plain";}

            @Override
            public long toMappedNodeId(long o) {return o;}

            @Override
            public long toOriginalNodeId(long m) {return m;}

            @Override
            public boolean containsOriginalId(long o) {return o < 2;}

            @Override
            public long nodeCount() {return 2;}

            @Override
            public long highestOriginalId() {return 1;}
        };
        var lb = LabelInformationBuilders.multiLabelWithCapacity(2);
        lb.addNodeIdToLabel(A, 0);
        var idMap = ComposedIdMap.of(translator, lb.build(2, id -> id));
        Assertions.assertThatThrownBy(() -> idMap.withFilteredLabels(List.of(A), CONCURRENCY))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    private static ComposedIdMap idMap(long nodeCount, long originalIdOffset) {
        var mappedToOriginal = new long[(int) nodeCount];
        var labelBuilder = LabelInformationBuilders.multiLabelWithCapacity(nodeCount);
        for (int mapped = 0; mapped < nodeCount; mapped++) {
            long originalId = originalIdOffset + mapped;
            mappedToOriginal[mapped] = originalId;
            labelBuilder.addNodeIdToLabel(mapped % 2 == 0 ? A : B, originalId);
        }
        var translator = new ListTranslator(mappedToOriginal);
        return ComposedIdMap.of(translator, labelBuilder.build(nodeCount, translator::toMappedNodeId));
    }
}
