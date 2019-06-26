/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.pagerank.PageRankFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.CentralityScore;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphalgo.impl.results.PageRankScore;
import org.neo4j.graphalgo.impl.utils.CentralityUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class PageRankProc extends BaseAlgoProc<PageRank> {

    public static final String CONFIG_DAMPING = "dampingFactor";

    public static final Double DEFAULT_DAMPING = 0.85;
    public static final Integer DEFAULT_ITERATIONS = 20;
    public static final String DEFAULT_SCORE_PROPERTY = "pagerank";

    public static final String CONFIG_WEIGHT_KEY = "weightProperty";


    @Procedure(value = "algo.pageRank", mode = Mode.WRITE)
    @Description("CALL algo.pageRank(label:String, relationship:String, " +
            "{iterations:5, dampingFactor:0.85, weightProperty: null, write: true, writeProperty:'pagerank', concurrency:4}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty" +
            " - calculates page rank and potentially writes back")
    public Stream<PageRankScore.Stats> pageRank(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationship, config);
        Graph graph = this.loadGraph(configuration, tracker, statsBuilder);
        statsBuilder.withNodes(graph.nodeCount());
        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(statsBuilder.build());
        }

        CentralityResult scores = compute(statsBuilder, tracker, configuration, graph);
        CentralityUtils.write(
                api,
                log,
                graph,
                TerminationFlag.wrap(transaction),
                scores,
                configuration,
                statsBuilder,
                DEFAULT_SCORE_PROPERTY);

        return Stream.of(statsBuilder.build());
    }

    @Procedure(value = "algo.pageRank.stream", mode = Mode.READ)
    @Description("CALL algo.pageRank.stream(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85, weightProperty: null, concurrency:4}) " +
            "YIELD node, score - calculates page rank and streams results")
    public Stream<CentralityScore> pageRankStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);
        Graph graph = this.loadGraph(configuration, tracker, statsBuilder);
        statsBuilder.withNodes(graph.nodeCount());
        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        CentralityResult scores = compute(statsBuilder, tracker, configuration, graph);
        return CentralityUtils.streamResults(graph, scores);
    }

    @Procedure(value = "algo.pageRank.memrec", mode = Mode.READ)
    @Description("CALL algo.pageRank.memrec(label:String, relationship:String, {...properties}) " +
            "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for PageRank")
    public Stream<MemRecResult> pageRankMemrec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    @Override
    GraphLoader configureLoader(final GraphLoader loader, final ProcedureConfiguration config) {
        loader.withOptionalRelationshipWeightsFromProperty(
                config.getString(CONFIG_WEIGHT_KEY, null),
                config.getWeightPropertyDefaultValue(0.0));

        Direction direction = config.getDirection(Direction.OUTGOING);
        if (direction == Direction.BOTH) {
            loader.asUndirected(true);
        } else {
            loader.withDirection(direction);
        }

        return loader;
    }

    @Override
    PageRank algorithm(
            final ProcedureConfiguration config,
            final AllocationTracker tracker,
            final Optional<Graph> graph) {

        final int batchSize = config.getBatchSize();
        final int concurrency = config.getConcurrency();

        double dampingFactor = config.get(CONFIG_DAMPING, DEFAULT_DAMPING);
        int iterations = config.getIterations(DEFAULT_ITERATIONS);
        PageRank.Config algoConfig = new PageRank.Config(iterations, dampingFactor);

        List<Node> sourceNodes = config.get("sourceNodes", Collections.emptyList());
        LongStream sourceNodeIds = sourceNodes.stream().mapToLong(Node::getId);

        // TODO: avoid null, maybe FakeGraph/EmptyGraph
        Graph theGraph = graph.orElse(null);
        final String weightPropertyKey = config.getString(CONFIG_WEIGHT_KEY, null);

        if (weightPropertyKey != null) {
            final boolean cacheWeights = config.get("cacheWeights", false);
            return PageRankFactory.weightedOf(
                    tracker,
                    theGraph,
                    algoConfig,
                    sourceNodeIds,
                    Pools.DEFAULT,
                    concurrency,
                    batchSize,
                    cacheWeights);
        } else {
            return PageRankFactory.of(
                    tracker,
                    theGraph,
                    algoConfig,
                    sourceNodeIds,
                    Pools.DEFAULT,
                    concurrency,
                    batchSize);
        }
    }

    private CentralityResult compute(
            final PageRankScore.Stats.Builder statsBuilder,
            final AllocationTracker tracker,
            final ProcedureConfiguration configuration,
            final Graph graph) {
        PageRank algo = newAlgorithm(configuration, tracker, Optional.of(graph));

        statsBuilder.timeEval(algo::compute);
        statsBuilder.withIterations(algo.iterations()).withDampingFactor(algo.dampingFactor());

        final CentralityResult scores = algo.result();
        algo.release();
        graph.release();

        log.info("PageRank: overall memory usage: %s", tracker.getUsageString());

        return scores;
    }
}
