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

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.similarity.specificfields.KnnSpecificFields;
import org.neo4j.gds.algorithms.RelationshipMutateResult;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFields;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnMutateConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.knn.KnnMutateConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.similarity.SimilarityResultCompanion.FILTERED_KNN_SPECIFIC_FIELDS_SUPPLIER;
import static org.neo4j.gds.algorithms.similarity.SimilarityResultCompanion.KNN_SPECIFIC_FIELDS_SUPPLIER;
import static org.neo4j.gds.algorithms.similarity.SimilarityResultCompanion.NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER;

public class SimilarityAlgorithmsMutateBusinessFacade {

    private final SimilarityAlgorithmsFacade similarityAlgorithmsFacade;
    private final MutateRelationshipService mutateRelationshipService;

    public SimilarityAlgorithmsMutateBusinessFacade(
        SimilarityAlgorithmsFacade similarityAlgorithmsFacade,
        MutateRelationshipService mutateRelationshipService
    ) {
        this.similarityAlgorithmsFacade = similarityAlgorithmsFacade;
        this.mutateRelationshipService = mutateRelationshipService;
    }

    public RelationshipMutateResult nodeSimilarity(
        String graphName,
        NodeSimilarityMutateConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.nodeSimilarity(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutate(
            algorithmResult,
            configuration,
            result -> result.graphResult(),
            NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER,
            intermediateResult.computeMilliseconds,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            computeSimilarityDistribution,
            configuration.mutateRelationshipType(),
            configuration.mutateProperty()
        );

    }

    public RelationshipMutateResult<SimilaritySpecificFieldsWithDistribution> filteredNodeSimilarity(
        String graphName,
        FilteredNodeSimilarityMutateConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.filteredNodeSimilarity(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutate(
            algorithmResult,
            configuration,
            result -> result.graphResult(),
            NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER,
            intermediateResult.computeMilliseconds,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            computeSimilarityDistribution,
            configuration.mutateRelationshipType(),
            configuration.mutateProperty()
        );
    }

    public RelationshipMutateResult knn(
        String graphName,
        KnnMutateConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.knn(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutate(
            algorithmResult,
            configuration,
            result -> SimilarityResultCompanion.computeToGraph(
                algorithmResult.graph(),
                algorithmResult.graph().nodeCount(),
                configuration.concurrency(),
                result.streamSimilarityResult()
            ),
            KNN_SPECIFIC_FIELDS_SUPPLIER,
            intermediateResult.computeMilliseconds,
            () -> KnnSpecificFields.EMPTY,
            computeSimilarityDistribution,
            configuration.mutateRelationshipType(),
            configuration.mutateProperty()
        );

    }

    public RelationshipMutateResult filteredKnn(
        String graphName,
        FilteredKnnMutateConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.filteredKnn(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutate(
            algorithmResult,
            configuration,
            result -> SimilarityResultCompanion.computeToGraph(
                algorithmResult.graph(),
                algorithmResult.graph().nodeCount(),
                configuration.concurrency(),
                result.similarityResultStream()
            ),
            FILTERED_KNN_SPECIFIC_FIELDS_SUPPLIER,
            intermediateResult.computeMilliseconds,
            () -> KnnSpecificFields.EMPTY,
            computeSimilarityDistribution,
            configuration.mutateRelationshipType(),
            configuration.mutateProperty()
        );

    }

    <RESULT, ASF extends SimilaritySpecificFields, CONFIG extends AlgoBaseConfig> RelationshipMutateResult<ASF> mutate(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        Function<RESULT, SimilarityGraphResult> similarityGraphResultSupplier,
        SpecificFieldsWithSimilarityStatisticsSupplier<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        boolean shouldComputeSimilarityDistribution,
        String mutateRelationshipType,
        String mutateProperty
    ) {

        return algorithmResult.result().map(result -> {

            var similaritySingleTypeRelationshipsHandler = new SimilaritySingleTypeRelationshipsHandler(
                algorithmResult.graph(),
                () -> similarityGraphResultSupplier.apply(result),
                shouldComputeSimilarityDistribution
            );

            var mutateResult = mutateRelationshipService.mutate(
                algorithmResult.graphStore(),
                mutateRelationshipType,
                mutateProperty,
                similaritySingleTypeRelationshipsHandler
            );

            var specificFields = specificFieldsSupplier.specificFields(
                result,
                similaritySingleTypeRelationshipsHandler.similaritySummary()
            );

            return RelationshipMutateResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .mutateMillis(mutateResult.mutateMilliseconds())
                .relationshipsWritten(mutateResult.relationshipsAdded())
                .algorithmSpecificFields(specificFields)
                .configuration(configuration)
                .build();
            
        }).orElseGet(() -> RelationshipMutateResult.empty(emptyASFSupplier.get(), configuration));

    }
}
