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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.NeighborhoodSampler;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.api.Graph;

import java.util.List;
import java.util.stream.LongStream;

public interface Layer {
    int sampleSize();

    Aggregator aggregator();

    default long randomState() {
        return sampler().randomState();
    }

    NeighborhoodSampler sampler();

    default List<Weights<? extends Tensor<?>>> weights() {
        return aggregator().weights();
    }

    default LongStream neighborhoodFunction(Graph graph, long nodeId) {
        return sampler().sample(graph, nodeId, sampleSize());
    }
}
