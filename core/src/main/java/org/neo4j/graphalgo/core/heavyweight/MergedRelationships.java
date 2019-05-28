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

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.heavyweight.HeavyGraph.checkSize;

public class MergedRelationships {
    private final AdjacencyMatrix matrix;
    private boolean hasRelationshipWeights;
    private DuplicateRelationshipsStrategy duplicateRelationshipsStrategy;

    public MergedRelationships(
            int nodeCount,
            GraphSetup setup,
            DuplicateRelationshipsStrategy duplicateRelationshipsStrategy) {
        this.matrix = new AdjacencyMatrix(
                nodeCount,
                setup.shouldLoadRelationshipWeight(),
                setup.relationDefaultWeight,
                false,
                setup.tracker);
        this.hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy;
    }

    public boolean canMerge(Relationships result) {
        return result.rows() > 0;
    }

    public void merge(Relationships result) {
        result.matrix().nodesWithRelationships(Direction.OUTGOING).forEachNode(
                node -> {
                    result.matrix().forEach(node, Direction.OUTGOING, (source, target, weight) -> {
                        checkSize(source, target);
                        duplicateRelationshipsStrategy.handle(
                                (int) source,
                                (int) target,
                                matrix,
                                hasRelationshipWeights,
                                weight);
                        return true;
                    });
                    return true;
                });
    }

    public AdjacencyMatrix matrix() {
        return matrix;
    }
}
