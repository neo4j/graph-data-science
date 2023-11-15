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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComponentIteratorTest {
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

        var nodesSortedByComponent = HugeLongArray.newArray(8);
        // uniqueIdx, nodeId
        nodesSortedByComponent.set(0, 2);
        nodesSortedByComponent.set(1, 1);
        nodesSortedByComponent.set(2, 0);
        nodesSortedByComponent.set(3, 7);
        nodesSortedByComponent.set(4, 6);
        nodesSortedByComponent.set(5, 5);
        nodesSortedByComponent.set(6, 4);
        nodesSortedByComponent.set(7, 3);

        var upperBoundPerComponent = HugeAtomicLongArray.of(8, ParalleLongPageCreator.passThrough(4));
        // componentId, upperBound
        upperBoundPerComponent.set(0, 2);
        upperBoundPerComponent.set(1, 7);

        ComponentIterator iterator = new ComponentIterator(1, nodesSortedByComponent, upperBoundPerComponent);

        for (int nodeId = 7; nodeId > 2; nodeId--) {
            assertTrue(iterator.hasNext());
            assertEquals(nodeId, iterator.next());
        }
        assertFalse(iterator.hasNext());
    }
}
