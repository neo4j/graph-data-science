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
package org.neo4j.gds.ml.negativeSampling;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

public class UserInputNegativeSampler implements NegativeSampler {
    private final Graph negativeExampleGraph;
    private final long testRelationshipCount;

    public UserInputNegativeSampler(
        Graph negativeExampleGraph,
        long testRelationshipCount
    ) {
        this.negativeExampleGraph = negativeExampleGraph;
        this.testRelationshipCount = testRelationshipCount;
    }

    @Override
    public void produceNegativeSamples(
        RelationshipsBuilder testSetBuilder,
        RelationshipsBuilder trainSetBuilder
    ) {
        var negativeSampleCount = new MutableLong(0);

        negativeExampleGraph.forEachNode(nodeId -> {
            negativeExampleGraph.forEachRelationship(nodeId, (s, t) -> {
                // add each relationship only once, even in UNDIRECTED graphs
                //TODO Add randomness for splitting given graph
                if (s < t) {
                    if (negativeSampleCount.getAndIncrement() < testRelationshipCount) {
                        testSetBuilder.add(s, t, NEGATIVE);
                    } else {
                        trainSetBuilder.add(s, t, NEGATIVE);
                    }
                }
                return true;
            });
            return negativeSampleCount.getValue() < negativeExampleGraph.relationshipCount();
        });
    }
}
