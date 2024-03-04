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
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.similarity.specificfields.KnnSpecificFields;
import org.neo4j.gds.procedures.similarity.knn.KnnMutateResult;
import org.neo4j.gds.procedures.similarity.knn.KnnStatsResult;
import org.neo4j.gds.procedures.similarity.knn.KnnWriteResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnMutateConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStatsConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnWriteConfig;

import java.util.stream.Stream;

final class FilteredKnnComputationResultTransformer {
    private FilteredKnnComputationResultTransformer() {}

    static Stream<SimilarityResult> toStreamResult(
        StreamComputationResult<FilteredKnnResult> computationResult
    ) {
        return computationResult.result().map(result -> {
            var graph = computationResult.graph();
            return result.similarityResultStream()
                .map(similarityResult -> {
                    similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                    similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                    return similarityResult;
                });
        }).orElseGet(Stream::empty);
    }

    static KnnStatsResult toStatsResult(
        StatsResult<KnnSpecificFields> statsResult,
        FilteredKnnStatsConfig config
    ) {

        return new KnnStatsResult(
            statsResult.preProcessingMillis(),
            statsResult.computeMillis(),
            statsResult.postProcessingMillis(),
            statsResult.algorithmSpecificFields().nodesCompared(),
            statsResult.algorithmSpecificFields().relationshipsWritten(),
            statsResult.algorithmSpecificFields().similarityDistribution(),
            statsResult.algorithmSpecificFields().didConverge(),
            statsResult.algorithmSpecificFields().ranIterations(),
            statsResult.algorithmSpecificFields().nodePairsConsidered(),
            config.toMap()
        );

    }

    static KnnMutateResult toMutateResult(
        RelationshipMutateResult<KnnSpecificFields> mutateResult,
        FilteredKnnMutateConfig config
    ) {

        return new KnnMutateResult(
            mutateResult.preProcessingMillis(),
            mutateResult.computeMillis(),
            mutateResult.mutateMillis(),
            mutateResult.postProcessingMillis(),
            mutateResult.algorithmSpecificFields().nodesCompared(),
            mutateResult.algorithmSpecificFields().relationshipsWritten(),
            mutateResult.algorithmSpecificFields().similarityDistribution(),
            mutateResult.algorithmSpecificFields().didConverge(),
            mutateResult.algorithmSpecificFields().ranIterations(),
            mutateResult.algorithmSpecificFields().nodePairsConsidered(),
            config.toMap()
        );
    }

    static KnnWriteResult toWriteResult(
        RelationshipWriteResult<KnnSpecificFields> writeResult,
        FilteredKnnWriteConfig config
    ) {

        return new KnnWriteResult(
            writeResult.preProcessingMillis(),
            writeResult.computeMillis(),
            writeResult.writeMillis(),
            writeResult.postProcessingMillis(),
            writeResult.algorithmSpecificFields().nodesCompared(),
            writeResult.algorithmSpecificFields().relationshipsWritten(),
            writeResult.algorithmSpecificFields().didConverge(),
            writeResult.algorithmSpecificFields().ranIterations(),
            writeResult.algorithmSpecificFields().nodePairsConsidered(),
            writeResult.algorithmSpecificFields().similarityDistribution(),
            config.toMap()
        );
    }


}
