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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.shuffle.ShuffleUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodesSortedByComponentTest {

    @Test
    void shouldDetermineIndexUpperBound() {
        // nodeId -> componentId
        var components = new LongUnaryOperator() {
            @Override
            public long applyAsLong(long nodeId) {
                if (nodeId < 3) {
                    return 0L; // size 3
                } else if (nodeId < 8) {
                    return 1L; // size 5
                } else if (nodeId < 12) {
                    return 2L; // size 4
                } else if (nodeId < 18) {
                    return 3L; // size 6
                } else if (nodeId < 19) {
                    return 4L; // size 1
                } else if (nodeId < 21) {
                    return 5L; // size 2
                } else {
                    return 6L; // size 7
                }
            }
        };


        var idxUpperBoundPerComponent = NodesSortedByComponent.computeIndexUpperBoundPerComponent(components, 28, 4);
        // we cannot infer which component follows another, but the size must match for the component
        Map<Long, Long> componentPerIdxUpperBound = new HashMap<>(7);
        for (int i = 0; i < 7; i++) {
            componentPerIdxUpperBound.put(idxUpperBoundPerComponent.get(i), (long) i);
        }
        assertEquals(0, idxUpperBoundPerComponent.get(7));
        int lowerBound = 0;
        for (long key : componentPerIdxUpperBound.keySet().stream().sorted().toList()) {
            switch ((int) (key - lowerBound)) {
                case 1: assertEquals(4, componentPerIdxUpperBound.get(key));break;
                case 2: assertEquals(5, componentPerIdxUpperBound.get(key));break;
                case 3: assertEquals(0, componentPerIdxUpperBound.get(key));break;
                case 4: assertEquals(2, componentPerIdxUpperBound.get(key));break;
                case 5: assertEquals(1, componentPerIdxUpperBound.get(key));break;
                case 6: assertEquals(3, componentPerIdxUpperBound.get(key));break;
                case 7: assertEquals(6, componentPerIdxUpperBound.get(key));break;
            }
            lowerBound = (int) (key);
        }
    }

    @Test
    void shouldReturnNodesForComponent() {
        // nodeId -> componentId
        var components = new LongUnaryOperator() {
            @Override
            public long applyAsLong(long nodeId) {
                if (nodeId < 3) {
                    return 0L; // size 3
                } else {
                    return 1L; // size 5
                }
            }
        };

        var nodesSorted = HugeLongArray.newArray(8);
        // uniqueIdx, nodeId
        nodesSorted.set(0, 2);
        nodesSorted.set(1, 1);
        nodesSorted.set(2, 0);
        nodesSorted.set(3, 7);
        nodesSorted.set(4, 6);
        nodesSorted.set(5, 5);
        nodesSorted.set(6, 4);
        nodesSorted.set(7, 3);

        var upperBoundPerComponent = HugeAtomicLongArray.of(8, ParalleLongPageCreator.passThrough(4));
        // componentId, upperBound
        upperBoundPerComponent.set(0, 3);
        upperBoundPerComponent.set(1, 8);

        NodesSortedByComponent nodesSortedByComponentMock = Mockito.mock(NodesSortedByComponent.class);
        Mockito.doReturn(components).when(nodesSortedByComponentMock).getComponents();
        Mockito.doReturn(upperBoundPerComponent).when(nodesSortedByComponentMock).getUpperBoundPerComponent();
        Mockito.doReturn(nodesSorted).when(nodesSortedByComponentMock).getNodesSorted();
        Mockito.doCallRealMethod().when(nodesSortedByComponentMock).iterator(1L,-1L);

        Iterator<Long> iterator = nodesSortedByComponentMock.iterator(1L, -1L);
        for (int nodeId = 3; nodeId < 8; nodeId++) {
            assertTrue(iterator.hasNext());
            assertEquals(nodeId, iterator.next());
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    void shouldRespectOffset() {
        LongUnaryOperator components = nodeId -> 0L;

        var nodesSorted = HugeLongArray.newArray(20);
        nodesSorted.setAll(x -> x);
        ShuffleUtil.shuffleArray(nodesSorted, new SplittableRandom(92));

        var upperBoundPerComponent = HugeAtomicLongArray.of(1, ParalleLongPageCreator.passThrough(4));
        upperBoundPerComponent.set(0, 20);

        NodesSortedByComponent nodesSortedByComponentMock = Mockito.mock(NodesSortedByComponent.class);
        Mockito.doReturn(components).when(nodesSortedByComponentMock).getComponents();
        Mockito.doReturn(upperBoundPerComponent).when(nodesSortedByComponentMock).getUpperBoundPerComponent();
        Mockito.doReturn(nodesSorted).when(nodesSortedByComponentMock).getNodesSorted();
        Mockito.doCallRealMethod().when(nodesSortedByComponentMock).iterator(0L,11L);

        Set<Long> resultingNodes = new HashSet<>();
        Iterator<Long> iterator = nodesSortedByComponentMock.iterator(0L, 11L);
        iterator.forEachRemaining(resultingNodes::add);
        assertThat(resultingNodes).containsExactlyInAnyOrder(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L);
    }
}
