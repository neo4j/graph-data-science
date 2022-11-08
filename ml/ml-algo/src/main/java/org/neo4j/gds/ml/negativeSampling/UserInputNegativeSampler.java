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

import java.util.Optional;
import java.util.SplittableRandom;

public class UserInputNegativeSampler implements NegativeSampler {
    private final Graph negativeExampleGraph;
    private final double testTrainFraction;

    private final SplittableRandom rng;

    public UserInputNegativeSampler(
        Graph negativeExampleGraph,
        double testTrainFraction,
        Optional<Long> randomSeed
    ) {
        if (!negativeExampleGraph.schema().isUndirected()) {
            throw new IllegalArgumentException("UserInputNegativeSampler requires graph to be UNDIRECTED.");
        }
        this.negativeExampleGraph = negativeExampleGraph;
        this.testTrainFraction = testTrainFraction;
        this.rng = randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
    }

    @Override
    public void produceNegativeSamples(
        RelationshipsBuilder testSetBuilder,
        RelationshipsBuilder trainSetBuilder
    ) {
        var totalRelationshipCount = negativeExampleGraph.relationshipCount()/2;
        var testRelationshipCount = (long) (totalRelationshipCount * testTrainFraction);
        var testRelationshipsToAdd = new MutableLong(testRelationshipCount);
        var trainRelationshipsToAdd = new MutableLong(totalRelationshipCount - testRelationshipCount);

        negativeExampleGraph.forEachNode(nodeId -> {
            negativeExampleGraph.forEachRelationship(nodeId, (s, t) -> {
                if (s < t) {
                    if ((rng.nextDouble() < 0.5 && testRelationshipsToAdd.longValue() > 0) || trainRelationshipsToAdd.longValue() == 0) {
                        testRelationshipsToAdd.decrement();
                        testSetBuilder.add(s, t, NEGATIVE);
                    } else {
                        trainRelationshipsToAdd.decrement();
                        trainSetBuilder.add(s, t, NEGATIVE);
                    }
                }
                return true;
            });
            return true;
        });
    }
}
