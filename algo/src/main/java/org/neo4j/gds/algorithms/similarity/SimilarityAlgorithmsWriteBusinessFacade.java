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
package org.neo4j.gds.algorithms.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.RelationshipWriteResult;
import org.neo4j.gds.algorithms.SimilaritySpecificFields;
import org.neo4j.gds.algorithms.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityWriteConfig;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

public class SimilarityAlgorithmsWriteBusinessFacade {

    private final SimilarityAlgorithmsFacade similarityAlgorithmsFacade;
    private final WriteRelationshipService writeRelationshipService;

    public SimilarityAlgorithmsWriteBusinessFacade(
        SimilarityAlgorithmsFacade similarityAlgorithmsFacade,
        WriteRelationshipService writeRelationshipService
    ) {
        this.similarityAlgorithmsFacade = similarityAlgorithmsFacade;
        this.writeRelationshipService = writeRelationshipService;
    }

    public RelationshipWriteResult nodeSimilarity(
        String graphName,
        NodeSimilarityWriteConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.nodeSimilarity(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return write(
            algorithmResult,
            configuration,
            result -> result.graphResult(),
            ((result, similarityDistribution) -> {
                var graphResult = result.graphResult();
                return new SimilaritySpecificFieldsWithDistribution(
                    graphResult.comparedNodes(),
                    graphResult.similarityGraph().relationshipCount(),
                    similarityDistribution
                );
            }),
            intermediateResult.computeMilliseconds,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            computeSimilarityDistribution,
            "NodeSimilarityWrite",
            configuration.writeProperty(),
            configuration.writeRelationshipType(),
            configuration.arrowConnectionInfo()
        );


    }

    <RESULT, ASF extends SimilaritySpecificFields, CONFIG extends AlgoBaseConfig> RelationshipWriteResult<ASF> write(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        Function<RESULT, SimilarityGraphResult> similarityGraphResultSupplier,
        SpecificFieldsWithSimilarityStatisticsSupplier<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        boolean shouldComputeSimilarityDistribution,
        String taskName,
        String writeProperty,
        String writeRelationshipType,
        Optional<WriteConfig.ArrowConnectionInfo> arrowConnectionInfo
    ) {

        return algorithmResult.result().map(result -> {

            var similarityGraphResult = similarityGraphResultSupplier.apply(result);
            var similarityGraph = similarityGraphResult.similarityGraph();

            var rootIdMap = similarityGraphResult.isTopKGraph()
                ? similarityGraph
                : algorithmResult.graphStore().nodes();

            // 2. Compute result statistics (but also write)
            Map<String, Object> similaritySummary;
            WriteRelationshipResult writeResult;

            if (shouldComputeSimilarityDistribution) {
                DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
                writeResult = writeRelationshipService.write(
                    writeRelationshipType,
                    writeProperty,
                    similarityGraph, algorithmResult.graphStore(),
                    rootIdMap,
                    taskName,
                    algorithmResult.algorithmTerminationFlag().get(),
                    arrowConnectionInfo,
                    (node1, node2, similarity) -> {
                        histogram.recordValue(similarity);
                        return true;
                    }

                );

                similaritySummary = SimilarityStatistics.similaritySummary(Optional.of(histogram));

            } else {
                writeResult = writeRelationshipService.write(
                    writeRelationshipType,
                    writeProperty,
                    similarityGraph, algorithmResult.graphStore(),
                    rootIdMap,
                    taskName,
                    algorithmResult.algorithmTerminationFlag().get(),
                    arrowConnectionInfo
                );
                similaritySummary = Map.of();

            }
            
            var specificFields = specificFieldsSupplier.specificFields(result, similaritySummary);

            return RelationshipWriteResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .writeMillis(writeResult.writeMilliseconds())
                .relationshipsWritten(writeResult.relationshipsWritten())
                .postProcessingMillis(0) //everything seems to happen in write-millis time
                .algorithmSpecificFields(specificFields)
                .configuration(configuration)
                .build();

        }).orElseGet(() -> RelationshipWriteResult.empty(emptyASFSupplier.get(), configuration));

    }
}
