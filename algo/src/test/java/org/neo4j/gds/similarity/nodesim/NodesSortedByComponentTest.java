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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodesSortedByComponentTest {

    @Test
    void shouldDetermineIndexUpperBound() {
        var components = HugeLongArray.newArray(28);
        // nodeId, componentId
        for (int nodeId = 0; nodeId < 3; nodeId++) {
            components.set(nodeId, 0); // size 3
        }
        for (int nodeId = 3; nodeId < 8; nodeId++) {
            components.set(nodeId, 1); // size 5
        }
        for (int nodeId = 8; nodeId < 12; nodeId++) {
            components.set(nodeId, 2); // size 4
        }
        for (int nodeId = 12; nodeId < 18; nodeId++) {
            components.set(nodeId, 3); // size 6
        }
        components.set(18, 4); // size 1
        for (int nodeId = 19; nodeId < 21; nodeId++) {
            components.set(nodeId, 5); // size 2
        }
        for (int nodeId = 21; nodeId < 28; nodeId++) {
            components.set(nodeId, 6); // size 7
        }

        var idxUpperBoundPerComponent = NodesSortedByComponent.computeIndexUpperBoundPerComponent(components, 4);
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
        var components = HugeLongArray.newArray(8);
        // nodeId, componentId
        for (int nodeId = 0; nodeId < 3; nodeId++) {
            components.set(nodeId, 0);
        }
        for (int nodeId = 3; nodeId < 8; nodeId++) {
            components.set(nodeId, 1);
        }

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
        Mockito.doCallRealMethod().when(nodesSortedByComponentMock).iterator(1L);

        Iterator<Long> iterator = nodesSortedByComponentMock.iterator(1L);
        for (int nodeId = 3; nodeId < 8; nodeId++) {
            assertTrue(iterator.hasNext());
            assertEquals(nodeId, iterator.next());
        }
        assertFalse(iterator.hasNext());
    }
}
