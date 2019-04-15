/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * @author mknobloch
 */
@RunWith(MockitoJUnitRunner.class)
public class AdjacencyMatrixSortingTest {

    private static RelationshipConsumer relationConsumer = mock(RelationshipConsumer.class);

    @Before
    public void resetMocks() {
        Mockito.reset(relationConsumer);
    }


    @Test
    public void sortOutgoing() throws Exception {
        AdjacencyMatrix matrix = new AdjacencyMatrix(3, false, AllocationTracker.EMPTY);
        matrix.addOutgoing(0, 1);
        matrix.addOutgoing(0, 2);

        matrix.sortOutgoing(0);

        matrix.forEach(0, Direction.OUTGOING, relationConsumer);

        verify(relationConsumer, times(1)).accept(eq(0), eq(1), eq(RawValues.combineIntInt(0, 1)));
        verify(relationConsumer, times(1)).accept(eq(0), eq(2), eq(RawValues.combineIntInt(0, 2)));
    }

    @Test
    public void sortIncoming() throws Exception {
        AdjacencyMatrix matrix = new AdjacencyMatrix(3, false, AllocationTracker.EMPTY);
        matrix.addIncoming(1, 0);
        matrix.addIncoming(2, 0);

        matrix.sortIncoming(0);

        matrix.forEach(0, Direction.INCOMING, relationConsumer);

        verify(relationConsumer, times(1)).accept(eq(0), eq(1), eq(RawValues.combineIntInt(1, 0)));
        verify(relationConsumer, times(1)).accept(eq(0), eq(2), eq(RawValues.combineIntInt(2, 0)));
    }

}
