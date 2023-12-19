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
package org.neo4j.gds.procedures.embeddings;

import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.TrainResult;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageMutateResult;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageStreamResult;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageTrainResult;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageWriteResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

class GraphSageComputationalResultTransformer {

    static Stream<GraphSageStreamResult> toStreamResult(
        StreamComputationResult<GraphSageResult> computationResult
    ) {
        return computationResult.result().map(graphSageResult -> {
            var graph = computationResult.graph();
            var embeddings = graphSageResult.embeddings();
            return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                .mapToObj(internalNodeId -> new GraphSageStreamResult(
                    graph.toOriginalNodeId(internalNodeId),
                    embeddings.get(internalNodeId)
                ));

        }).orElseGet(Stream::empty);
    }

    static GraphSageMutateResult toMutateResult(NodePropertyMutateResult<Long> mutateResult) {

        return new GraphSageMutateResult(
            mutateResult.algorithmSpecificFields().longValue(),
            mutateResult.nodePropertiesWritten(),
            mutateResult.preProcessingMillis(),
            mutateResult.computeMillis(),
            mutateResult.mutateMillis(),
            mutateResult.configuration().toMap()
        );

    }

    static GraphSageWriteResult toWriteResult(NodePropertyWriteResult<Long> writeResult) {

        return new GraphSageWriteResult(
            writeResult.algorithmSpecificFields().longValue(),
            writeResult.nodePropertiesWritten(),
            writeResult.preProcessingMillis(),
            writeResult.computeMillis(),
            writeResult.writeMillis(),
            writeResult.configuration().toMap()
        );

    }
    static GraphSageTrainResult toTrainResult(
        TrainResult<Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>> trainResult
    ) {

        return new GraphSageTrainResult(trainResult.algorithmSpecificFields(), trainResult.trainMillis());
    }
}
