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
package org.neo4j.gds.ml.core.subgraph;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.ml.core.RelationshipWeights;
import org.neo4j.gds.ml.core.samplers.UniformSampler;
import org.neo4j.gds.ml.core.samplers.WeightedUniformSampler;

import java.util.stream.LongStream;

public class NeighborhoodSampler {
    // Influence of the weight for the probability
    private long randomSeed;

    public NeighborhoodSampler(long randomSeed) {
        this.randomSeed = randomSeed;
    }

    public LongStream sample(Graph graph, long nodeId, int numberOfSamples) {
        var degree = graph.degree(nodeId);

        var concurrentCopyGraph = graph.concurrentCopy();
        // Every neighbor needs to be sampled
        if (degree <= numberOfSamples) {
            return concurrentCopyGraph
                .streamRelationships(nodeId, RelationshipWeights.DEFAULT_VALUE)
                .mapToLong(RelationshipCursor::targetId);
        }

        if (graph.hasRelationshipProperty()) {
            return new WeightedUniformSampler(randomSeed)
                .sample(
                    concurrentCopyGraph.streamRelationships(nodeId, RelationshipWeights.DEFAULT_VALUE),
                    degree,
                    numberOfSamples
                );
        } else {
            return new UniformSampler(randomSeed).sample(
                concurrentCopyGraph.streamRelationships(nodeId, RelationshipWeights.DEFAULT_VALUE),
                degree,
                numberOfSamples
            );
        }
    }

    public long randomState() {
        return this.randomSeed;
    }

    public void changeRandomState() {
        this.randomSeed++;
    }
}
