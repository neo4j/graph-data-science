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
package org.neo4j.gds.procedures.similarity;

import org.neo4j.gds.algorithms.RelationshipMutateResult;
import org.neo4j.gds.algorithms.RelationshipWriteResult;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityMutateResult;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityStatsResult;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityWriteResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsConfig;

import java.util.stream.Stream;

final class NodeSimilarityComputationResultTransformer {
    private NodeSimilarityComputationResultTransformer() {}

    static Stream<SimilarityResult> toStreamResult(
        StreamComputationResult<NodeSimilarityResult> computationResult
    ) {
        return computationResult.result().map(result -> {
            var graph = computationResult.graph();
            return result.streamResult()
                .map(similarityResult -> {
                    similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                    similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                    return similarityResult;
                });
        }).orElseGet(Stream::empty);
    }

    static SimilarityStatsResult toStatsResult(
        StatsResult<SimilaritySpecificFieldsWithDistribution> statsResult,
        NodeSimilarityStatsConfig config
    ) {

        return new SimilarityStatsResult(
            statsResult.preProcessingMillis(),
            statsResult.computeMillis(),
            statsResult.postProcessingMillis(),
            statsResult.algorithmSpecificFields().nodesCompared(),
            statsResult.algorithmSpecificFields().relationshipsWritten(),
            statsResult.algorithmSpecificFields().similarityDistribution(),
            config.toMap()
        );
    }

    static SimilarityWriteResult toWriteResult(
        RelationshipWriteResult<SimilaritySpecificFieldsWithDistribution> writeResult
    ) {

        return new SimilarityWriteResult(
            writeResult.preProcessingMillis(),
            writeResult.computeMillis(),
            writeResult.writeMillis(),
            writeResult.postProcessingMillis(),
            writeResult.algorithmSpecificFields().nodesCompared(),
            writeResult.algorithmSpecificFields().relationshipsWritten(),
            writeResult.algorithmSpecificFields().similarityDistribution(),
            writeResult.configuration().toMap()
        );
    }

    static SimilarityMutateResult toMutateResult(
        RelationshipMutateResult<SimilaritySpecificFieldsWithDistribution> mutateResult
    ) {

        return new SimilarityMutateResult(
            mutateResult.preProcessingMillis(),
            mutateResult.computeMillis(),
            mutateResult.mutateMillis(),
            mutateResult.postProcessingMillis(),
            mutateResult.algorithmSpecificFields().nodesCompared(),
            mutateResult.algorithmSpecificFields().relationshipsWritten(),
            mutateResult.algorithmSpecificFields().similarityDistribution(),
            mutateResult.configuration().toMap()
        );
    }


}
