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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.Collection;
import java.util.Optional;

public interface NegativeSampler {

    public static final double NEGATIVE = 0D;

    static NegativeSampler of(
        GraphStore graphStore,
        Graph graph,
        Optional<String> negativeRelationshipType,
        double negativeSamplingRatio,
        long testPositiveCount,
        long trainPositiveCount,
        IdMap validSourceNodes,
        IdMap validTargetNodes,
        Collection<NodeLabel> sourceLabels,
        Collection<NodeLabel> targetLabels,
        Optional<Long> randomSeed
    ) {
        if (negativeRelationshipType.isPresent()) {
            Graph negativeExampleGraph = graphStore.getGraph(RelationshipType.of(negativeRelationshipType.orElseThrow()));
            double testTrainFraction = testPositiveCount / (double) (testPositiveCount + trainPositiveCount);

            return new UserInputNegativeSampler(
                negativeExampleGraph,
                testTrainFraction,
                randomSeed,
                sourceLabels,
                targetLabels
            );
        } else {
            return new RandomNegativeSampler(
                graph,
                (long) (testPositiveCount * negativeSamplingRatio),
                (long) (trainPositiveCount * negativeSamplingRatio),
                validSourceNodes,
                validTargetNodes,
                randomSeed
            );
        }
    }

    void produceNegativeSamples(RelationshipsBuilder testSetBuilder, RelationshipsBuilder trainSetBuilder);

}
