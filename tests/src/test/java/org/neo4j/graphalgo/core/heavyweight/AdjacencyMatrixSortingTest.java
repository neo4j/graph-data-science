/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.heavyweight;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * @author mknobloch
 */
@RunWith(MockitoJUnitRunner.class)
public class AdjacencyMatrixSortingTest {

    @Test
    public void sortOutgoing() {
        RelationshipConsumer consumer = mock(RelationshipConsumer.class);
        AdjacencyMatrix matrix = new AdjacencyMatrix(3, false, 0D, false, AllocationTracker.EMPTY);
        matrix.addOutgoing(0, 2);
        matrix.addOutgoing(0, 1);

        matrix.sortOutgoing(0);

        matrix.forEach(0, Direction.OUTGOING, consumer);

        InOrder order = inOrder(consumer);
        order.verify(consumer).accept(0, 1);
        order.verify(consumer).accept(0, 2);
        order.verifyNoMoreInteractions();
    }

    @Test
    public void sortOutgoingWithWeights() {
        WeightedRelationshipConsumer consumer = mock(WeightedRelationshipConsumer.class);

        AdjacencyMatrix matrix = new AdjacencyMatrix(3, true, 0D, false, AllocationTracker.EMPTY);
        matrix.addOutgoingWithWeight(0, 2, 2.0);
        matrix.addOutgoingWithWeight(0, 1, 1.0);

        matrix.sortOutgoing(0);

        matrix.forEach(0, Direction.OUTGOING, consumer);

        InOrder order = inOrder(consumer);
        order.verify(consumer).accept(0, 1, 1.0);
        order.verify(consumer).accept(0, 2, 2.0);
        order.verifyNoMoreInteractions();
    }

    @Test
    public void sortIncoming() {
        RelationshipConsumer consumer = mock(RelationshipConsumer.class);
        AdjacencyMatrix matrix = new AdjacencyMatrix(3, false, 0D, false, AllocationTracker.EMPTY);
        matrix.addIncoming(2, 0);
        matrix.addIncoming(1, 0);

        matrix.sortIncoming(0);

        matrix.forEach(0, Direction.INCOMING, consumer);

        InOrder order = inOrder(consumer);
        order.verify(consumer).accept(0, 1);
        order.verify(consumer).accept(0, 2);
        order.verifyNoMoreInteractions();
    }

    @Test
    public void sortIncomingWithWeights() {
        WeightedRelationshipConsumer consumer = mock(WeightedRelationshipConsumer.class);
        AdjacencyMatrix matrix = new AdjacencyMatrix(3, true, 0D, false, AllocationTracker.EMPTY);
        matrix.addIncomingWithWeight(2, 0, 2.0);
        matrix.addIncomingWithWeight(1, 0, 1.0);

        matrix.sortIncoming(0);

        matrix.forEach(0, Direction.INCOMING, consumer);

        InOrder order = inOrder(consumer);
        order.verify(consumer).accept(0, 1, 1.0);
        order.verify(consumer).accept(0, 2, 2.0);
        order.verifyNoMoreInteractions();
    }

}
