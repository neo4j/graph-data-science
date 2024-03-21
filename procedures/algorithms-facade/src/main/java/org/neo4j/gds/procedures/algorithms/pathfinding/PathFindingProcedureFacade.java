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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.ResultBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.traverse.BfsStatsConfig;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.BreadthFirstSearchMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.GenericStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathAStarMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathDijkstraMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathYensMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SingleSourceShortestPathDijkstraMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SteinerTreeMutateStub;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;
import org.neo4j.gds.steiner.SteinerTreeStatsConfig;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This is the top facade on the Neo4j Procedures integration for path finding algorithms.
 * The role it plays is, to be newed up with request scoped dependencies,
 * and to capture the procedure-specific bits of path finding algorithms calls.
 * For example, translating a return column specification into a parameter, a business level concept.
 * This is also where we put result rendering.
 */
public final class PathFindingProcedureFacade {
    // request scoped services
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final ConfigurationCreator configurationCreator;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;

    // delegate
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeFacade;
    private final PathFindingAlgorithmsStatsModeBusinessFacade statsModeFacade;
    private final PathFindingAlgorithmsStreamModeBusinessFacade streamModeFacade;
    private final PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade;

    // applications
    private final BreadthFirstSearchMutateStub breadthFirstSearchMutateStub;
    private final SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub;
    private final SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub;
    private final SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub;
    private final SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub;
    private final SteinerTreeMutateStub steinerTreeMutateStub;

    private PathFindingProcedureFacade(
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeFacade,
        PathFindingAlgorithmsStatsModeBusinessFacade statsModeFacade,
        PathFindingAlgorithmsStreamModeBusinessFacade streamModeFacade,
        PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade,
        BreadthFirstSearchMutateStub breadthFirstSearchMutateStub,
        SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub,
        SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub,
        SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub,
        SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub,
        SteinerTreeMutateStub steinerTreeMutateStub
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.configurationCreator = configurationCreator;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;

        this.estimationModeFacade = estimationModeFacade;
        this.statsModeFacade = statsModeFacade;
        this.streamModeFacade = streamModeFacade;
        this.writeModeFacade = writeModeFacade;

        this.breadthFirstSearchMutateStub = breadthFirstSearchMutateStub;
        this.singlePairShortestPathAStarMutateStub = singlePairShortestPathAStarMutateStub;
        this.singlePairShortestPathDijkstraMutateStub = singlePairShortestPathDijkstraMutateStub;
        this.singlePairShortestPathYensMutateStub = singlePairShortestPathYensMutateStub;
        this.singleSourceShortestPathDijkstraMutateStub = singleSourceShortestPathDijkstraMutateStub;
        this.steinerTreeMutateStub = steinerTreeMutateStub;
    }

    /**
     * Encapsulating some of the boring structure stuff
     */
    public static PathFindingProcedureFacade create(
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator,
        ConfigurationParser configurationParser,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        User user,
        PathFindingAlgorithmsEstimationModeBusinessFacade pathFindingAlgorithmsEstimationModeBusinessFacade,
        PathFindingAlgorithmsMutateModeBusinessFacade pathFindingAlgorithmsMutateModeBusinessFacade,
        PathFindingAlgorithmsStatsModeBusinessFacade pathFindingAlgorithmsStatsModeBusinessFacade,
        PathFindingAlgorithmsStreamModeBusinessFacade pathFindingAlgorithmsStreamModeBusinessFacade,
        PathFindingAlgorithmsWriteModeBusinessFacade pathFindingAlgorithmsWriteModeBusinessFacade
    ) {
        var genericStub = new GenericStub(
            defaultsConfiguration,
            limitsConfiguration,
            configurationCreator,
            configurationParser,
            user,
            pathFindingAlgorithmsEstimationModeBusinessFacade
        );

        var aStarStub = new SinglePairShortestPathAStarMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var singlePairDijkstraStub = new SinglePairShortestPathDijkstraMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var yensStub = new SinglePairShortestPathYensMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var singleSourceDijkstraStub = new SingleSourceShortestPathDijkstraMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var steinerTreeMutateStub = new SteinerTreeMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var breadthFirstSearchMutateStub = new BreadthFirstSearchMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        return new PathFindingProcedureFacade(
            closeableResourceRegistry,
            configurationCreator,
            nodeLookup,
            procedureReturnColumns,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsStatsModeBusinessFacade,
            pathFindingAlgorithmsStreamModeBusinessFacade,
            pathFindingAlgorithmsWriteModeBusinessFacade,
            breadthFirstSearchMutateStub,
            aStarStub,
            singlePairDijkstraStub,
            yensStub,
            singleSourceDijkstraStub,
            steinerTreeMutateStub
        );
    }

    public BreadthFirstSearchMutateStub breadthFirstSearchMutateStub() {
        return breadthFirstSearchMutateStub;
    }

    public Stream<StandardStatsResult> breadthFirstSearchStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new BfsStatsResultBuilder();

        return runStatsAlgorithm(
            graphName,
            configuration,
            BfsStatsConfig::of,
            resultBuilder,
            statsModeFacade::breadthFirstSearch
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            BfsStatsConfig::of,
            configuration -> estimationModeFacade.breadthFirstSearchEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<BfsStreamResult> breadthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new BfsStreamResultBuilder(nodeLookup, procedureReturnColumns.contains("path"));

        return runStreamAlgorithm(
            graphName,
            configuration,
            BfsStreamConfig::of,
            resultBuilder,
            streamModeFacade::breadthFirstSearchStream
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            BfsStreamConfig::of,
            configuration -> estimationModeFacade.breadthFirstSearchEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub() {
        return singlePairShortestPathAStarMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathAStarStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            ShortestPathAStarStreamConfig::of,
            streamModeFacade::singlePairShortestPathAStarStream
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathAStarStreamConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathAStarEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStarWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathAStarWriteConfig::of,
                writeModeFacade::singlePairShortestPathAStarWrite
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathAStarWriteConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathAStarEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub() {
        return singlePairShortestPathDijkstraMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            ShortestPathDijkstraStreamConfig::of,
            streamModeFacade::singlePairShortestPathDijkstraStream
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraStreamConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathDijkstraEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathDijkstraWriteConfig::of,
                writeModeFacade::singlePairShortestPathDijkstraWrite
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraWriteConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathDijkstraEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub() {
        return singlePairShortestPathYensMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathYensStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            ShortestPathYensStreamConfig::of,
            streamModeFacade::singlePairShortestPathYensStream
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathYensStreamConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathYensEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathYensWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathYensWriteConfig::of,
                writeModeFacade::singlePairShortestPathYensWrite
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathYensWriteConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathYensEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub() {
        return singleSourceShortestPathDijkstraMutateStub;
    }

    public Stream<PathFindingStreamResult> singleSourceShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            AllShortestPathsDijkstraStreamConfig::of,
            streamModeFacade::singleSourceShortestPathDijkstraStream
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            AllShortestPathsDijkstraStreamConfig::of,
            configuration -> estimationModeFacade.singleSourceShortestPathDijkstraEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singleSourceShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                AllShortestPathsDijkstraWriteConfig::of,
                writeModeFacade::singleSourceShortestPathDijkstraWrite
            )
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            AllShortestPathsDijkstraWriteConfig::of,
            configuration -> estimationModeFacade.singleSourceShortestPathDijkstraEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SteinerTreeMutateStub steinerTreeMutateStub() {
        return steinerTreeMutateStub;
    }

    public Stream<SteinerStatsResult> steinerTreeStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForStatsMode();

        return runStatsAlgorithm(
            graphName,
            configuration,
            SteinerTreeStatsConfig::of,
            resultBuilder,
            statsModeFacade::steinerTree
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SteinerTreeStatsConfig::of,
            configuration -> estimationModeFacade.steinerTreeEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SteinerTreeStreamResult> steinerTreeStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForStreamMode();

        return runStreamAlgorithm(
            graphName,
            configuration,
            SteinerTreeStreamConfig::of,
            resultBuilder,
            streamModeFacade::steinerTreeStream
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SteinerTreeStreamConfig::of,
            configuration -> estimationModeFacade.steinerTreeEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SteinerWriteResult> steinerTreeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForWriteMode();

        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                SteinerTreeWriteConfig::of,
                writeModeFacade::steinerTreeWrite,
                resultBuilder
            )
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SteinerTreeWriteConfig::of,
            configuration -> estimationModeFacade.steinerTreeEstimate(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    /**
     * Just a bit of reuse
     */
    private <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult runEstimation(
        Map<String, Object> algorithmConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationParser,
        Function<CONFIGURATION, MemoryEstimateResult> supplier
    ) {
        var configuration = configurationCreator.createConfiguration(
            algorithmConfiguration,
            configurationParser
        );

        return supplier.apply(configuration);
    }

    /**
     * A*, Dijkstra, Yens all share the same result builder
     */
    private <CONFIGURATION extends AlgoBaseConfig> Stream<PathFindingStreamResult> runPathOrientedAlgorithmInStreamMode(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, Stream<PathFindingStreamResult>> algorithm
    ) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<CONFIGURATION>(
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return runStreamAlgorithm(graphNameAsString, rawConfiguration, configurationSupplier, resultBuilder, algorithm);
    }

    private <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> Stream<RESULT_TO_CALLER> runStatsAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> resultBuilder,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, configurationSupplier);

        return algorithm.compute(graphName, configuration, resultBuilder);
    }

    /**
     * Some reuse, all the algorithms use the same high level structure:
     * <ol>
     *     <li> configuration parsing
     *     <li> parameter marshalling
     *     <li> delegating to down stream layer to call the thing we are actually interested in
     *     <li> handle resource closure
     * </ol>
     */
    private <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> Stream<RESULT_TO_CALLER> runStreamAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> resultBuilder,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfigurationForStream(rawConfiguration, configurationSupplier);

        var resultStream = algorithm.compute(graphName, configuration, resultBuilder);

        // we need to do this for stream mode
        closeableResourceRegistry.register(resultStream);

        return resultStream;
    }

    /**
     * A*, Dijkstra and Yens use the same variant of result builder
     */
    private <CONFIGURATION extends AlgoBaseConfig> StandardWriteRelationshipsResult runWriteAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, StandardWriteRelationshipsResult> algorithm
    ) {
        var resultBuilder = new PathFindingResultBuilderForWriteMode<CONFIGURATION>();

        return runWriteAlgorithm(
            graphNameAsString,
            rawConfiguration,
            configurationSupplier,
            algorithm,
            resultBuilder
        );
    }

    private <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> RESULT_TO_CALLER runWriteAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> algorithm,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, configurationSupplier);

        return algorithm.compute(graphName, configuration, resultBuilder);
    }
}
