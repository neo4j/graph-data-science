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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.pagerank.PageRankFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.CentralityScore;
import org.neo4j.graphalgo.impl.results.PageRankScore;
import org.neo4j.graphalgo.impl.utils.CentralityUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

//TODO: Add acceptance tests ("integration tests")
public final class ArticleRankProc {

    public static final String CONFIG_DAMPING = "dampingFactor";

    public static final Double DEFAULT_DAMPING = 0.85;
    public static final Integer DEFAULT_ITERATIONS = 20;
    public static final String DEFAULT_SCORE_PROPERTY = "articlerank";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.articleRank", mode = Mode.WRITE)
    @Description("CALL algo.articleRank(label:String, relationship:String, " +
            "{iterations:5, dampingFactor:0.85, weightProperty: null, write: true, writeProperty:'articlerank', concurrency:4}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty" +
            " - calculates page rank and potentially writes back")
    public Stream<PageRankScore.Stats> articleRank(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        if(graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(statsBuilder.build());
        }

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        CentralityResult scores = runAlgorithm(graph, tracker, terminationFlag, configuration, statsBuilder);

        log.info("ArticleRank: overall memory usage: %s", tracker.getUsageString());

        CentralityUtils.write(api, log, graph, terminationFlag, scores, configuration, statsBuilder, DEFAULT_SCORE_PROPERTY);

        return Stream.of(statsBuilder.build());
    }

    @Procedure(value = "algo.articleRank.stream", mode = Mode.READ)
    @Description("CALL algo.articleRank.stream(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85, weightProperty: null, concurrency:4}) " +
            "YIELD node, score - calculates page rank and streams results")
    public Stream<CentralityScore> articleRankStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        if(graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        CentralityResult scores = runAlgorithm(graph, tracker, terminationFlag, configuration, statsBuilder);

        log.info("ArticleRank: overall memory usage: %s", tracker.getUsageString());

        return CentralityUtils.streamResults(graph, scores);
    }

    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            PageRankScore.Stats.Builder statsBuilder,
            ProcedureConfiguration configuration) {
        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withoutRelationshipWeights();

        Direction direction = configuration.getDirection(Direction.OUTGOING);
        if (direction == Direction.BOTH) {
            graphLoader.asUndirected(true);
        } else {
            graphLoader.withDirection(direction);
        }


        try (ProgressTimer timer = statsBuilder.timeLoad()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }

    private CentralityResult runAlgorithm(
            Graph graph,
            AllocationTracker tracker,
            TerminationFlag terminationFlag,
            ProcedureConfiguration configuration,
            PageRankScore.Stats.Builder statsBuilder) {

        double dampingFactor = configuration.get(CONFIG_DAMPING, DEFAULT_DAMPING);
        int iterations = configuration.getIterations(DEFAULT_ITERATIONS);
        final int batchSize = configuration.getBatchSize();
        final int concurrency = configuration.getConcurrency();
        log.debug("Computing article rank with damping of " + dampingFactor + " and " + iterations + " iterations.");

        List<Node> sourceNodes = configuration.get("sourceNodes", new ArrayList<>());
        LongStream sourceNodeIds = sourceNodes.stream().mapToLong(Node::getId);

        Direction direction = (configuration.getDirection(Direction.OUTGOING) == Direction.BOTH) ?
                Direction.OUTGOING :
                configuration.getDirection(Direction.OUTGOING);

        PageRank prAlgo = PageRankFactory.articleRankOf(
                tracker,
                graph,
                direction,
                dampingFactor,
                sourceNodeIds,
                Pools.DEFAULT,
                concurrency,
                batchSize);

        Algorithm<?> algo = prAlgo
                .withLog(log)
                .withTerminationFlag(terminationFlag);

        statsBuilder.timeEval(() -> prAlgo.compute(iterations));

        statsBuilder
                .withIterations(iterations)
                .withDampingFactor(dampingFactor);

        final CentralityResult pageRank = prAlgo.result();
        algo.release();
        graph.release();
        return pageRank;
    }

}
