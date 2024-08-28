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
package org.neo4j.gds.procedures.algorithms.machinelearning;

import org.neo4j.gds.algorithms.machinelearning.KGEPredictResult;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictStreamConfig;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;

import java.util.Optional;
import java.util.stream.Stream;

class KgeResultBuilderForStreamMode implements StreamResultBuilder<KGEPredictStreamConfig, KGEPredictResult, KGEStreamResult> {
    @Override
    public Stream<KGEStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        KGEPredictStreamConfig configuration,
        Optional<KGEPredictResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var kgePredictResult = result.get();

        return kgePredictResult.topKMap().stream(
            (node1, node2, similarity) -> new KGEStreamResult(
                graph.toOriginalNodeId(node1),
                graph.toOriginalNodeId(node2),
                similarity
            )
        );
    }
}
