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
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class MergedRelationshipsTest {

    @Test
    public void byDefaultDontRemoveDuplicates() {
        AdjacencyMatrix matrix = new AdjacencyMatrix(2, false, 0d, false, AllocationTracker.EMPTY);
        matrix.addOutgoing(0, 1);

        Relationships relationships = new Relationships(0, 5, matrix);

        MergedRelationships mergedRelationships = new MergedRelationships(5, new GraphSetup(), DuplicateRelationshipsStrategy.NONE);

        mergedRelationships.merge(relationships);
        mergedRelationships.merge(relationships);

        assertEquals(2, mergedRelationships.matrix().degree(0, Direction.OUTGOING));
    }

    @Test
    public void skipRemovesDuplicates() {
        AdjacencyMatrix matrix = new AdjacencyMatrix(2, false, 0d, false, AllocationTracker.EMPTY);
        matrix.addOutgoing(0, 1);

        Relationships relationships = new Relationships(0, 5, matrix);

        MergedRelationships mergedRelationships = new MergedRelationships(5, new GraphSetup(), DuplicateRelationshipsStrategy.SKIP);

        mergedRelationships.merge(relationships);
        mergedRelationships.merge(relationships);

        assertEquals(1, mergedRelationships.matrix().degree(0, Direction.OUTGOING));
    }

    @Test
    public void sumRemovesDuplicates() {
        GraphSetup setup = graphSetupWithRelationships();

        MergedRelationships mergedRelationships = new MergedRelationships(5, setup, DuplicateRelationshipsStrategy.SUM);

        AdjacencyMatrix matrix1 = new AdjacencyMatrix(1, true, 0d, false, AllocationTracker.EMPTY);
        matrix1.addOutgoingWithWeight(0, 1, 3.0);
        Relationships relationships1 = new Relationships(0, 5, matrix1);

        AdjacencyMatrix matrix2 = new AdjacencyMatrix(1, true, 0d, false, AllocationTracker.EMPTY);
        matrix1.addOutgoingWithWeight(0, 1, 7.0);
        Relationships relationships2 = new Relationships(0, 5, matrix2);

        mergedRelationships.merge(relationships1);
        mergedRelationships.merge(relationships2);

        AdjacencyMatrix matrix = mergedRelationships.matrix();
        assertEquals(1, matrix.degree(0, Direction.OUTGOING));
        assertEquals(10.0, matrix.getOutgoingWeight(0, matrix.outgoingIndex(0, 1)), 0.01);
    }

    @Test
    public void minPicksLowestWeight() {
        MergedRelationships mergedRelationships = new MergedRelationships(5, graphSetupWithRelationships(), DuplicateRelationshipsStrategy.MIN);

        AdjacencyMatrix matrix1 = new AdjacencyMatrix(1, true, 0d, false, AllocationTracker.EMPTY);
        matrix1.addOutgoingWithWeight(0, 1, 3.0);
        Relationships relationships1 = new Relationships(0, 5, matrix1);

        AdjacencyMatrix matrix2 = new AdjacencyMatrix(1, true, 0d, false, AllocationTracker.EMPTY);
        matrix1.addOutgoingWithWeight(0, 1, 7.0);
        Relationships relationships2 = new Relationships(0, 5, matrix2);

        mergedRelationships.merge(relationships1);
        mergedRelationships.merge(relationships2);

        AdjacencyMatrix matrix = mergedRelationships.matrix();
        assertEquals(1, matrix.degree(0, Direction.OUTGOING));
        assertEquals(3.0, matrix.getOutgoingWeight(0, matrix.outgoingIndex(0, 1)), 0.01);
    }

    @Test
    public void maxPicksLargestWeight() {
        MergedRelationships mergedRelationships = new MergedRelationships(5, graphSetupWithRelationships(), DuplicateRelationshipsStrategy.MAX);

        AdjacencyMatrix matrix1 = new AdjacencyMatrix(1, true, 0d, false, AllocationTracker.EMPTY);
        matrix1.addOutgoingWithWeight(0, 1, 3.0);
        Relationships relationships1 = new Relationships(0, 5, matrix1);

        AdjacencyMatrix matrix2 = new AdjacencyMatrix(1, true, 0d, false, AllocationTracker.EMPTY);
        matrix1.addOutgoingWithWeight(0, 1, 7.0);
        Relationships relationships2 = new Relationships(0, 5, matrix2);

        mergedRelationships.merge(relationships1);
        mergedRelationships.merge(relationships2);

        AdjacencyMatrix matrix = mergedRelationships.matrix();
        assertEquals(1, matrix.degree(0, Direction.OUTGOING));
        assertEquals(7.0, matrix.getOutgoingWeight(0, matrix.outgoingIndex(0, 1)), 0.01);
    }

    private GraphSetup graphSetupWithRelationships() {
        return new GraphLoader(mock(GraphDatabaseAPI.class)).withRelationshipWeightsFromProperty("dummy", 0.0).toSetup();
    }


}
