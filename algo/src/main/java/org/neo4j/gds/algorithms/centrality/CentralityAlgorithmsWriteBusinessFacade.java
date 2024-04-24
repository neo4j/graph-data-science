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
import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.centrality.specificfields.AlphaHarmonicSpecificFields;
import org.neo4j.gds.algorithms.centrality.specificfields.CELFSpecificFields;
import org.neo4j.gds.algorithms.centrality.specificfields.CentralityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.centrality.specificfields.DefaultCentralitySpecificFields;
import org.neo4j.gds.algorithms.centrality.specificfields.PageRankSpecificFields;
import org.neo4j.gds.algorithms.runner.AlgorithmResultWithTiming;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.closeness.ClosenessCentralityWriteConfig;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.degree.DegreeCentralityWriteConfig;
import org.neo4j.gds.harmonic.DeprecatedTieredHarmonicCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.influenceMaximization.CELFNodeProperties;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationWriteConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankWriteConfig;
import org.neo4j.gds.result.CentralityStatistics;

import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.runner.AlgorithmRunner.runWithTiming;

public class CentralityAlgorithmsWriteBusinessFacade {

    private final CentralityAlgorithmsFacade centralityAlgorithmsFacade;
    private final WriteNodePropertyService writeNodePropertyService;

    public CentralityAlgorithmsWriteBusinessFacade(
        CentralityAlgorithmsFacade centralityAlgorithmsFacade,
        WriteNodePropertyService writeNodePropertyService
    ) {
        this.centralityAlgorithmsFacade = centralityAlgorithmsFacade;
        this.writeNodePropertyService = writeNodePropertyService;
    }

    public NodePropertyWriteResult<DefaultCentralitySpecificFields> betweennessCentrality(
        String graphName,
        BetweennessCentralityWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.betweennessCentrality(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            "BetweennessCentralityWrite",
            configuration.writeConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(intermediateResult.algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<DefaultCentralitySpecificFields> degreeCentrality(
        String graphName,
        DegreeCentralityWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.degreeCentrality(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            "DegreeCentralityWrite",
            configuration.writeConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(intermediateResult.algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<DefaultCentralitySpecificFields> closenessCentrality(
        String graphName,
        ClosenessCentralityWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.closenessCentrality(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            "ClosenessCentralityWrite",
            configuration.writeConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(intermediateResult.algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<PageRankSpecificFields> pageRank(
        String graphName,
        PageRankWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.pageRank(graphName, configuration)
        );

        return pageRankVariant(intermediateResult, configuration, shouldComputeCentralityDistribution);
    }

    public NodePropertyWriteResult<PageRankSpecificFields> articleRank(
        String graphName,
        PageRankWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.articleRank(graphName, configuration)
        );

        return pageRankVariant(
            intermediateResult, configuration,
            shouldComputeCentralityDistribution
        );
    }

    public NodePropertyWriteResult<PageRankSpecificFields> eigenvector(
        String graphName,
        PageRankWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.eigenvector(graphName, configuration)
        );

        return pageRankVariant(
            intermediateResult, configuration,
            shouldComputeCentralityDistribution
        );
    }

    @NotNull
    private NodePropertyWriteResult<PageRankSpecificFields> pageRankVariant(
        AlgorithmResultWithTiming<AlgorithmComputationResult<PageRankResult>> intermediateResult,
        PageRankWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        var algorithmResult = intermediateResult.algorithmResult;
        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = result.nodePropertyValues();

            // 3. Write to database
            var graphStore = algorithmResult.graphStore();
            var writeNodePropertyResult = writeNodePropertyService.write(
                algorithmResult.graph(),
                graphStore,
                nodePropertyValues,
                configuration.writeConcurrency(),
                configuration.writeProperty(),
                "PageRankWrite",
                configuration.arrowConnectionInfo(),
                configuration.resolveResultStore(algorithmResult.resultStore())
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

            return NodePropertyWriteResult.<PageRankSpecificFields>builder()
                .computeMillis(intermediateResult.computeMilliseconds)
                .postProcessingMillis(pageRankDistribution.postProcessingMillis)
                .nodePropertiesWritten(writeNodePropertyResult.nodePropertiesWritten())
                .writeMillis(writeNodePropertyResult.writeMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyWriteResult.empty(PageRankSpecificFields.EMPTY, configuration));
    }

    public NodePropertyWriteResult<DefaultCentralitySpecificFields> harmonicCentrality(
        String graphName,
        HarmonicCentralityWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.harmonicCentrality(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            "HarmonicCentralityWrite",
            configuration.writeConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(intermediateResult.algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<AlphaHarmonicSpecificFields> alphaHarmonicCentrality(
        String graphName,
        DeprecatedTieredHarmonicCentralityWriteConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> centralityAlgorithmsFacade.harmonicCentrality(graphName, configuration)
        );

        var algorithmResult = intermediateResult.algorithmResult;
        return writeToDatabase(
            algorithmResult,
            configuration,
            CentralityAlgorithmResult::centralityScoreProvider,
            CentralityAlgorithmResult::nodePropertyValues,
            (result, centralityDistribution) -> new AlphaHarmonicSpecificFields(
                centralityDistribution,
                intermediateResult.algorithmResult.graph().nodeCount()
            ),
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds,
            () -> AlphaHarmonicSpecificFields.EMPTY,
            "HarmonicCentralityWrite",
            configuration.writeConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );

    }

    public NodePropertyWriteResult<CELFSpecificFields> celf(
        String graphName,
        InfluenceMaximizationWriteConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> centralityAlgorithmsFacade.celf(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        var writeResultBuilder = NodePropertyWriteResult.<CELFSpecificFields>builder()
            .computeMillis(intermediateResult.computeMilliseconds)
            .postProcessingMillis(0L)
            .configuration(configuration);

        algorithmResult.result().ifPresentOrElse(
            result -> {
                var nodeCount = algorithmResult.graph().nodeCount();
                var nodeProperties = new CELFNodeProperties(result.seedSetNodes(), nodeCount);
                var writeResult = writeNodePropertyService.write(
                    algorithmResult.graph(),
                    algorithmResult.graphStore(),
                    nodeProperties,
                    configuration.writeConcurrency(),
                    configuration.writeProperty(),
                    "CELFWrite",
                    configuration.arrowConnectionInfo(),
                    configuration.resolveResultStore(algorithmResult.resultStore())
                );
                writeResultBuilder.writeMillis(writeResult.writeMilliseconds());
                writeResultBuilder.nodePropertiesWritten(writeResult.nodePropertiesWritten());
                writeResultBuilder.algorithmSpecificFields(new CELFSpecificFields(result.totalSpread(), nodeCount));
            },
            () -> writeResultBuilder.algorithmSpecificFields(CELFSpecificFields.EMPTY)
        );

        return writeResultBuilder.build();
    }

    private <RESULT extends CentralityAlgorithmResult, CONFIG extends AlgoBaseConfig> NodePropertyWriteResult<DefaultCentralitySpecificFields> writeToDatabase(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds,
        String procedureName,
        Concurrency writeConcurrency,
        String writeProperty,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore
    ) {

        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier = CentralityAlgorithmResult::centralityScoreProvider;
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, DefaultCentralitySpecificFields> specificFieldsSupplier = (r, c) -> new DefaultCentralitySpecificFields(
            c);
        Supplier<DefaultCentralitySpecificFields> emptyASFSupplier = () -> DefaultCentralitySpecificFields.EMPTY;

        NodePropertyValuesMapper<RESULT> nodePropertyValuesMapper = CentralityAlgorithmResult::nodePropertyValues;

        return writeToDatabase(
            algorithmResult,
            configuration,
            centralityFunctionSupplier,
            nodePropertyValuesMapper,
            specificFieldsSupplier,
            shouldComputeCentralityDistribution,
            computeMilliseconds,
            emptyASFSupplier,
            procedureName,
            writeConcurrency,
            writeProperty,
            arrowConnectionInfo,
            resultStore
        );
    }

    <RESULT, CONFIG extends AlgoBaseConfig, ASF extends CentralityStatisticsSpecificFields> NodePropertyWriteResult<ASF> writeToDatabase(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier,
        NodePropertyValuesMapper<RESULT> nodePropertyValuesMapper,
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, ASF> specificFieldsSupplier,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        String procedureName,
        Concurrency writeConcurrency,
        String writeProperty,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore
    ) {

        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = nodePropertyValuesMapper.map(
                result
            );

            // 3. Write to database
            var writeNodePropertyResult = writeNodePropertyService.write(
                algorithmResult.graph(),
                algorithmResult.graphStore(),
                nodePropertyValues,
                writeConcurrency,
                writeProperty,
                procedureName,
                arrowConnectionInfo,
                resultStore
            );

            // Compute result statistics
            var centralityStatistics = CentralityStatistics.centralityStatistics(
                algorithmResult.graph().nodeCount(),
                centralityFunctionSupplier.centralityFunction(result),
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                shouldComputeCentralityDistribution
            );

            var centralitySummary = CentralityStatistics.centralitySummary(centralityStatistics.histogram());

            var specificFields = specificFieldsSupplier.specificFields(result, centralitySummary);

            return NodePropertyWriteResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(centralityStatistics.computeMilliseconds())
                .nodePropertiesWritten(writeNodePropertyResult.nodePropertiesWritten())
                .writeMillis(writeNodePropertyResult.writeMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyWriteResult.empty(emptyASFSupplier.get(), configuration));

    }
}
