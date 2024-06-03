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
package org.neo4j.gds.algorithms.centrality;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.centrality.specificfields.PageRankSpecificFields;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.algorithms.runner.AlgorithmResultWithTiming;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.result.CentralityStatistics;

import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.runner.AlgorithmRunner.runWithTiming;

public class CentralityAlgorithmsMutateBusinessFacade {

    private final CentralityAlgorithmsFacade centralityAlgorithmsFacade;
    private final MutateNodePropertyService mutateNodePropertyService;

    public CentralityAlgorithmsMutateBusinessFacade(
        CentralityAlgorithmsFacade centralityAlgorithmsFacade,
        MutateNodePropertyService mutateNodePropertyService
    ) {
        this.centralityAlgorithmsFacade = centralityAlgorithmsFacade;
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    public NodePropertyMutateResult<PageRankSpecificFields> pageRank(
        String graphName,
        PageRankMutateConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.pageRank(graphName, configuration)
        );

        return pageRankVariant(intermediateResult, configuration, shouldComputeCentralityDistribution);
    }

    @NotNull
    private NodePropertyMutateResult<PageRankSpecificFields> pageRankVariant(
        AlgorithmResultWithTiming<AlgorithmComputationResult<PageRankResult>> intermediateResult, PageRankMutateConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        var algorithmResult = intermediateResult.algorithmResult;
        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = result.nodePropertyValues();

            // 3. Go and mutate the graph store
            var addNodePropertyResult = mutateNodePropertyService.mutate(
                configuration.mutateProperty(),
                nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                algorithmResult.graph(),
                algorithmResult.graphStore()
            );

            var pageRankDistribution = PageRankDistributionComputer.computeDistribution(
                result,
                configuration,
                shouldComputeCentralityDistribution
            );

            var specificFields = new PageRankSpecificFields(
                result.iterations(),
                result.didConverge(),
                pageRankDistribution.centralitySummary
            );

            return NodePropertyMutateResult.<PageRankSpecificFields>builder()
                .computeMillis(intermediateResult.computeMilliseconds)
                .postProcessingMillis(pageRankDistribution.postProcessingMillis)
                .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
                .mutateMillis(addNodePropertyResult.mutateMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyMutateResult.empty(PageRankSpecificFields.EMPTY, configuration));

    }

    <RESULT, CONFIG extends MutateNodePropertyConfig, ASF> NodePropertyMutateResult<ASF> mutateNodeProperty(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier,
        NodePropertyValuesMapper<RESULT> nodePropertyValuesMapper,
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, ASF> specificFieldsSupplier,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {
        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result

            var nodePropertyValues = nodePropertyValuesMapper.map(result);


            // Compute result statistics
            var centralityStatistics = CentralityStatistics.centralityStatistics(
                algorithmResult.graph().nodeCount(),
                centralityFunctionSupplier.centralityFunction(result),
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                shouldComputeCentralityDistribution
            );

            var centralitySummary = CentralityStatistics.centralitySummary(centralityStatistics.histogram());


            // 3. Go and mutate the graph store
            var addNodePropertyResult = mutateNodePropertyService.mutate(
                configuration.mutateProperty(),
                nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                algorithmResult.graph(),
                algorithmResult.graphStore()
            );

            var specificFields = specificFieldsSupplier.specificFields(result, centralitySummary);

            return NodePropertyMutateResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(0)
                .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
                .mutateMillis(addNodePropertyResult.mutateMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyMutateResult.empty(emptyASFSupplier.get(), configuration));

    }

}
