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
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphdb.Direction;

import java.util.function.Supplier;

import static org.neo4j.graphalgo.core.heavyweight.CypherLoadingUtils.newWeightMapping;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.ESTIMATED_DEGREE;

public class MergedRelationships implements RelationshipConsumer {
    private final AdjacencyMatrix matrix;
    private final WeightMap relWeights;
    private boolean hasRelationshipWeights;
    private DuplicateRelationshipsStrategy duplicateRelationshipsStrategy;

    public MergedRelationships(int nodeCount, GraphSetup setup, DuplicateRelationshipsStrategy duplicateRelationshipsStrategy) {
        this.matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);
        this.relWeights = newWeightMapping(
                setup.shouldLoadRelationshipWeight(),
                setup.relationDefaultWeight,
                nodeCount * ESTIMATED_DEGREE);
        this.hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy;
    }

    public boolean canMerge(Relationships result) {
        return result.rows() > 0;
    }

    public void merge(Relationships result) {
        WeightMap resultWeights = hasRelationshipWeights && result.relWeights().size() > 0 ? result.relWeights() : null;
        result.matrix().nodesWithRelationships(Direction.OUTGOING).forEachNode(
                node -> {
                    result.matrix().forEach(node, Direction.OUTGOING, (source, target, relationship) -> {
                        Supplier<Number> weightSupplier = () -> resultWeights.get(relationship);
                        duplicateRelationshipsStrategy.handle(source, target, matrix, resultWeights != null, relWeights, weightSupplier);
                        return true;
                    });
                    return true;
                });
    }

    public AdjacencyMatrix matrix() {
        return matrix;
    }

    public WeightMap relWeights() {
        return relWeights;
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
        return false;
    }
}
