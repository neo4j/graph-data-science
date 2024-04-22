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
import org.neo4j.gds.algorithms.RelationshipMutateResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFields;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.config.MutateRelationshipPropertyConfig;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.function.Function;
import java.util.function.Supplier;

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
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.nodeSimilarity(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutate(
            algorithmResult,
            configuration,
            NodeSimilarityResult::graphResult,
            NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER,
            intermediateResult.computeMilliseconds,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            computeSimilarityDistribution
        );

    }

    public RelationshipMutateResult<SimilaritySpecificFieldsWithDistribution> filteredNodeSimilarity(
        String graphName,
        FilteredNodeSimilarityMutateConfig configuration,
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.filteredNodeSimilarity(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutate(
            algorithmResult,
            configuration,
            NodeSimilarityResult::graphResult,
            NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER,
            intermediateResult.computeMilliseconds,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            computeSimilarityDistribution
        );
    }

    <RESULT, ASF extends SimilaritySpecificFields, CONFIG extends MutateRelationshipConfig & MutateRelationshipPropertyConfig> RelationshipMutateResult<ASF> mutate(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        Function<RESULT, SimilarityGraphResult> similarityGraphResultSupplier,
        SpecificFieldsWithSimilarityStatisticsSupplier<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        boolean shouldComputeSimilarityDistribution
    ) {

        return algorithmResult.result().map(result -> {

            var similaritySingleTypeRelationshipsHandler = new SimilaritySingleTypeRelationshipsHandler(
                algorithmResult.graph(),
                () -> similarityGraphResultSupplier.apply(result),
                shouldComputeSimilarityDistribution
            );

            var mutateResult = mutateRelationshipService.mutate(
                algorithmResult.graphStore(),
                configuration.mutateRelationshipType(),
                configuration.mutateProperty(),
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
