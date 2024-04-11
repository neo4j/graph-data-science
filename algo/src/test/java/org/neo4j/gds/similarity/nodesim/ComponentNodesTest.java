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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.shuffle.ShuffleUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComponentNodesTest {

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
        var idxUpperBoundPerComponent = ComponentNodes.computeIndexUpperBoundPerComponent(
            components,
            28,
            (v) -> true,
            new Concurrency(4)
        );
        // we cannot infer which component follows another, but the range must match in size for the component
        Map<Long, Long> componentPerIdxUpperBound = new HashMap<>(7);
        for (int i = 0; i < 7; i++) {
            componentPerIdxUpperBound.put(idxUpperBoundPerComponent.get(i), (long) i);
        }
        assertThat(idxUpperBoundPerComponent.get(7)).isEqualTo(-1L);
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

        var nodesSortedByComponent = ComponentNodes.computeNodesSortedByComponent(components,
            upperBoundPerComponent, (v) -> true, new Concurrency(4)
        );

        // nodes may occur in arbitrary order within components, but with the given assignment, nodeIds must be within
        // component index bounds
        assertEquals(28, nodesSortedByComponent.size());
        for (int i = 0; i < 28; i++) {
            var currentComp = components.applyAsLong(nodesSortedByComponent.get(i));

            assertThat(nodesSortedByComponent.get(i)).isGreaterThan(currentComp == 0 ?
                -1 : upperBoundPerComponent.get(currentComp - 1));
            assertThat(nodesSortedByComponent.get(i)).isLessThanOrEqualTo(upperBoundPerComponent.get(currentComp));
        }
    }

    @Test
    void shouldComputeNodesSortedByComponentsNotConsecutive() {
        // nodeId -> componentId
        LongUnaryOperator components = nodeId -> {
            if (nodeId < 4) {
                return 3; // size 4
            } else if (nodeId < 6) {
                return 5; // size 2
            } else {
                return 1; // size 5
            }
        };
        // componentId, upperIdx of component
        var upperBoundPerComponent = HugeAtomicLongArray.of(11, ParalleLongPageCreator.passThrough(4));
        upperBoundPerComponent.set(3, 3);
        upperBoundPerComponent.set(5, 10);
        upperBoundPerComponent.set(1, 8);

        var nodesSortedByComponent = ComponentNodes.computeNodesSortedByComponent(components,
            upperBoundPerComponent,
            (v) -> true,
            new Concurrency(4)
        );

        // nodes may occur in arbitrary order within components, but with the given assignment, nodeIds must be within
        // component index bounds
        assertEquals(11, nodesSortedByComponent.size());
        Collection<Long> values = new ArrayList<>();
        int end = 0;
        while (end < 11) {
            int start = end;
            if (nodesSortedByComponent.get(start) < 4) {
                // next 4 nodes must be of component 3
                end += 4;
                values.addAll(List.of(0L, 1L, 2L, 3L));
            } else if (nodesSortedByComponent.get(start) < 6) {
                // next 2 nodes must be of component 5
                end += 2;
                values.addAll(List.of(4L, 5L));
            } else {
                // next 5 nodes must be of component 1
                end += 5;
                values.addAll(List.of(6L, 7L, 8L, 9L, 10L));
            }
            for (int i = start; i < end; i++) {
                long nodeId = nodesSortedByComponent.get(i);
                assertTrue(values.remove(nodeId));
            }
        }

        ComponentNodes componentNodesMock = Mockito.mock(ComponentNodes.class);
        Mockito.doReturn(components).when(componentNodesMock).getComponents();
        Mockito.doReturn(upperBoundPerComponent).when(componentNodesMock).getUpperBoundPerComponent();
        Mockito.doReturn(nodesSortedByComponent).when(componentNodesMock).getNodesSorted();

        // no component with id 0
        Mockito.doCallRealMethod().when(componentNodesMock).iterator(0L,0L);
        Iterator<Long> iterator = componentNodesMock.iterator(0L, 0L);
        assertFalse(iterator.hasNext());

        // 5 nodes for component with id 1
        Mockito.doCallRealMethod().when(componentNodesMock).iterator(1L,0L);
        iterator = componentNodesMock.iterator(1L, 0L);
        values.addAll(List.of(6L, 7L, 8L, 9L, 10L));
        for (int i = 0; i < 5; i++) {
            assertTrue(iterator.hasNext());
            long nodeId = iterator.next();
            assertTrue(values.remove(nodeId));
        }
        assertFalse(iterator.hasNext());
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

        ComponentNodes componentNodesMock = Mockito.mock(ComponentNodes.class);
        Mockito.doReturn(components).when(componentNodesMock).getComponents();
        Mockito.doReturn(upperBoundPerComponent).when(componentNodesMock).getUpperBoundPerComponent();
        Mockito.doReturn(nodesSorted).when(componentNodesMock).getNodesSorted();

        // first component
        Mockito.doCallRealMethod().when(componentNodesMock).iterator(0L,0L);
        Iterator<Long> iterator = componentNodesMock.iterator(0L, 0L);
        for (int nodeId = 0; nodeId < 3; nodeId++) {
            assertTrue(iterator.hasNext());
            assertThat(iterator.next()).isEqualTo(nodeId);
        }
        assertFalse(iterator.hasNext());
        // second component
        Mockito.doCallRealMethod().when(componentNodesMock).iterator(1L,0L);
        iterator = componentNodesMock.iterator(1L, 0L);
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

        ComponentNodes componentNodesMock = Mockito.mock(ComponentNodes.class);
        Mockito.doReturn(components).when(componentNodesMock).getComponents();
        Mockito.doReturn(upperBoundPerComponent).when(componentNodesMock).getUpperBoundPerComponent();
        Mockito.doReturn(nodesSorted).when(componentNodesMock).getNodesSorted();
        Mockito.doCallRealMethod().when(componentNodesMock).iterator(0L,11L);

        Set<Long> resultingNodes = new HashSet<>();
        Iterator<Long> iterator = componentNodesMock.iterator(0L, 11L);
        iterator.forEachRemaining(resultingNodes::add);
        assertThat(resultingNodes).containsExactlyInAnyOrder(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L);
    }

    @Test
    void shouldComputeUpperIndexWithTargetFilter() {
        HugeLongArray components = HugeLongArray.of(0, 3, 0, 3, 5, 5, 5, 7);
        LongPredicate includeFilter = (v) -> v >= 3;

        var upperIndex = ComponentNodes.computeIndexUpperBoundPerComponent(
            components::get,
            components.size(),
            includeFilter,
            new Concurrency(4)
        );
        assertThat(upperIndex.get(0)).isEqualTo(-1L);
        assertThat(upperIndex.get(1)).isEqualTo(-1L);
        assertThat(upperIndex.get(2)).isEqualTo(-1L);
        assertThat(upperIndex.get(4)).isEqualTo(-1L);
        assertThat(upperIndex.get(6)).isEqualTo(-1L);

        TreeSet<Long> treeSet = new TreeSet<>();
        treeSet.add(-1L);
        for (int i = 0; i < upperIndex.size(); ++i) {
            if (upperIndex.get(i) >= 0) {
                treeSet.add(upperIndex.get(i));
            }
        }
        //3
        long upperBound3 = upperIndex.get(3);
        long sizeOfThree = upperBound3 - treeSet.lower(upperBound3).longValue();
        assertThat(sizeOfThree).isEqualTo(1);
        //5
        long upperBound5 = upperIndex.get(5);
        long sizeOfFive = upperBound5 - treeSet.lower(upperBound5).longValue();
        assertThat(sizeOfFive).isEqualTo(3);
        //7
        long upperBound7 = upperIndex.get(7);
        long sizeOfSeven = upperBound7 - treeSet.lower(upperBound7).longValue();
        assertThat(sizeOfSeven).isEqualTo(1);
    }

    @Test
    void shouldGenerateValidIterators() {
        HugeLongArray components = HugeLongArray.of(0, 3, 0, 3, 5, 5, 5, 7, 8, 8, 8);
        LongPredicate includeFilter = (v) -> v >= 3 && v != 9;

        var componentNodes = ComponentNodes.create(components::get, includeFilter, components.size(), new Concurrency(4));
        assertThat(componentNodes.iterator(0, 0).hasNext()).isFalse();
        assertThat(componentNodes.iterator(1, 0).hasNext()).isFalse();
        assertThat(componentNodes.iterator(2, 0).hasNext()).isFalse();
        assertThat(componentNodes.iterator(4, 0).hasNext()).isFalse();
        assertThat(componentNodes.iterator(6, 0).hasNext()).isFalse();
        assertThat(componentNodes.iterator(9, 0).hasNext()).isFalse();
        assertThat(componentNodes.iterator(10, 0).hasNext()).isFalse();

        var nodesOfThree = new ArrayList<Long>();
        Iterator<Long> itr3 = componentNodes.iterator(3, 0);
        itr3.forEachRemaining(nodesOfThree::add);
        assertThat(nodesOfThree).containsExactly(3L);

        var nodesOfFive = new ArrayList<Long>();
        Iterator<Long> itr5 = componentNodes.iterator(5, 0);
        itr5.forEachRemaining(nodesOfFive::add);
        assertThat(nodesOfFive).containsExactlyInAnyOrder(4l, 5l, 6l);

        var nodesOfSeven = new ArrayList<Long>();
        Iterator<Long> itr7 = componentNodes.iterator(7, 0);
        itr7.forEachRemaining(nodesOfSeven::add);
        assertThat(nodesOfSeven).containsExactly(7L);

        var nodesOfEight = new ArrayList<Long>();
        Iterator<Long> itr8 = componentNodes.iterator(8, 0);
        itr8.forEachRemaining(nodesOfEight::add);
        assertThat(nodesOfEight).containsExactlyInAnyOrder(8L, 10L);
    }

}
