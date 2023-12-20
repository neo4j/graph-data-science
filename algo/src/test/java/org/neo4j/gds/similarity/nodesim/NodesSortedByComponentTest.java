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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodesSortedByComponentTest {

    private LongUnaryOperator prepare7DistinctSizeComponents() {
        return nodeId -> {
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
        };
    }

    @Test
    void shouldDetermineIndexUpperBound() {
        // nodeId -> componentId
        var components = prepare7DistinctSizeComponents();
        // componentId, upperBound
        var idxUpperBoundPerComponent = NodesSortedByComponent.computeIndexUpperBoundPerComponent(components, 28, 4);
        // we cannot infer which component follows another, but the range must match in size for the component
        Map<Long, Long> componentPerIdxUpperBound = new HashMap<>(7);
        for (int i = 0; i < 7; i++) {
            componentPerIdxUpperBound.put(idxUpperBoundPerComponent.get(i), (long) i);
        }
        assertThat(idxUpperBoundPerComponent.get(7)).isEqualTo(0L);
        int previousUpperBound = -1;
        for (long key : componentPerIdxUpperBound.keySet().stream().sorted().collect(Collectors.toList())) {
            switch ((int) (key - previousUpperBound)) {
                case 1: assertThat(componentPerIdxUpperBound.get(key)).isEqualTo(4);break;
                case 2: assertThat(componentPerIdxUpperBound.get(key)).isEqualTo(5);break;
                case 3: assertThat(componentPerIdxUpperBound.get(key)).isEqualTo(0);break;
                case 4: assertThat(componentPerIdxUpperBound.get(key)).isEqualTo(2);break;
                case 5: assertThat(componentPerIdxUpperBound.get(key)).isEqualTo(1);break;
                case 6: assertThat(componentPerIdxUpperBound.get(key)).isEqualTo(3);break;
                case 7: assertThat(componentPerIdxUpperBound.get(key)).isEqualTo(6);break;
            }
            previousUpperBound = (int) (key);
        }
    }

    @Test
    void shouldComputeNodesSortedByComponent() {
        // nodeId -> componentId
        var components = prepare7DistinctSizeComponents();
        // componentId, upperIdx of component
        var upperBoundPerComponent = HugeAtomicLongArray.of(28, ParalleLongPageCreator.passThrough(4));
        upperBoundPerComponent.set(0, 2);
        upperBoundPerComponent.set(1, 7);
        upperBoundPerComponent.set(2, 11);
        upperBoundPerComponent.set(3, 17);
        upperBoundPerComponent.set(4, 18);
        upperBoundPerComponent.set(5, 20);
        upperBoundPerComponent.set(6, 27);
        var componentCoordinateArray = HugeAtomicLongArray.of(28, ParalleLongPageCreator.passThrough(4));
        upperBoundPerComponent.copyTo(componentCoordinateArray, 28);

        var nodesSortedByComponent = NodesSortedByComponent.computeNodesSortedByComponent(components,
            componentCoordinateArray, 4);

        // nodes may occur in arbitrary order within components, but with the given assignment, nodeIds must be within
        // component index bounds
        assertEquals(28, nodesSortedByComponent.size());
        for (int i = 0; i < 27; i++) {
            var currentComp = components.applyAsLong(nodesSortedByComponent.get(i));

            assertThat(nodesSortedByComponent.get(i)).isGreaterThan(currentComp == 0 ?
                -1 : upperBoundPerComponent.get(currentComp - 1));
            assertThat(nodesSortedByComponent.get(i)).isLessThanOrEqualTo(upperBoundPerComponent.get(currentComp));
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
        upperBoundPerComponent.set(0, 2);
        upperBoundPerComponent.set(1, 7);

        NodesSortedByComponent nodesSortedByComponentMock = Mockito.mock(NodesSortedByComponent.class);
        Mockito.doReturn(components).when(nodesSortedByComponentMock).getComponents();
        Mockito.doReturn(upperBoundPerComponent).when(nodesSortedByComponentMock).getUpperBoundPerComponent();
        Mockito.doReturn(nodesSorted).when(nodesSortedByComponentMock).getNodesSorted();

        // first component
        Mockito.doCallRealMethod().when(nodesSortedByComponentMock).iterator(0L,-1L);
        Iterator<Long> iterator = nodesSortedByComponentMock.iterator(0L, -1L);
        for (int nodeId = 0; nodeId < 3; nodeId++) {
            assertTrue(iterator.hasNext());
            assertThat(iterator.next()).isEqualTo(nodeId);
        }
        assertFalse(iterator.hasNext());
        // second component
        Mockito.doCallRealMethod().when(nodesSortedByComponentMock).iterator(1L,-1L);
        iterator = nodesSortedByComponentMock.iterator(1L, -1L);
        for (int nodeId = 3; nodeId < 8; nodeId++) {
            assertTrue(iterator.hasNext());
            assertThat(iterator.next()).isEqualTo(nodeId);
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
        upperBoundPerComponent.set(0, 19);

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
