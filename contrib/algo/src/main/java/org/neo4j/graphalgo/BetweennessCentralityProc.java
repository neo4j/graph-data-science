/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.betweenness.*;
import org.neo4j.graphalgo.results.BetweennessCentralityProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Betweenness Centrality Algorithms
 *
 * all procedures accept {in, incoming, <, out, outgoing, >, both, <>} as direction
 *
 * @author mknblch
 */
public class BetweennessCentralityProc {

    public static final String DEFAULT_TARGET_PROPERTY = "centrality";
    public static final Direction DEFAULT_DIRECTION = Direction.OUTGOING;

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    /**
     * Randomized Approximate Brandes Algorithm
     * for approximating Betweenness Centrality
     *
     * optional Arguments:
     *  strategy:'degree'   Degree based randomization
     *  strategy:'random'   Randomized selection. Takes optional argument probability:double[0-1]
     *                      or use log10(nodeCount) / e^2 as default
     */
    @Procedure(value = "algo.betweenness.sampled.stream")
    @Description("CALL algo.betweenness.sampled.stream(label:String, relationship:String, {strategy:{'random', 'degree'}, probability:double, maxDepth:int, direction:String, concurrency:int}) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<BetweennessCentrality.Result> betweennessRABrandes(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withoutNodeProperties()
                .withDirection(configuration.getDirection(Direction.OUTGOING))
                .load(configuration.getGraphImpl());

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        final RABrandesBetweennessCentrality algo =
                new RABrandesBetweennessCentrality(graph, Pools.DEFAULT, configuration.getConcurrency(), strategy(configuration, graph))
                        .withTerminationFlag(TerminationFlag.wrap(transaction))
                        .withProgressLogger(ProgressLogger.wrap(log, "Randomized Approximate Brandes: BetweennessCentrality(parallel)"))
                        .withDirection(configuration.getDirection(Direction.OUTGOING))
                        .withMaxDepth(configuration.getNumber("maxDepth", Integer.MAX_VALUE).intValue())
                        .compute();

        graph.release();

        return algo.resultStream();
    }

    /**
     * Brandes Betweenness Centrality Algorithm
     *
     */
    @Procedure(value = "algo.betweenness.stream")
    @Description("CALL algo.betweenness.stream(label:String, relationship:String, {direction:'out', concurrency :4})" +
                 "YIELD nodeId, centrality - yields centrality for each node")
    public Stream<BetweennessCentrality.Result> betweennessStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withoutNodeProperties()
                .withDirection(configuration.getDirection(DEFAULT_DIRECTION))
                .load(configuration.getGraphImpl());

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        final int concurrency = configuration.getConcurrency();
        if (concurrency > 1) {
            final ParallelBetweennessCentrality algo =
                    new ParallelBetweennessCentrality(graph, Pools.DEFAULT, concurrency)
                            .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality"))
                            .withTerminationFlag(TerminationFlag.wrap(transaction))
                            .withDirection(configuration.getDirection(DEFAULT_DIRECTION))
                            .compute();
            graph.release();
            return algo.resultStream();
        }

        final BetweennessCentrality compute = new BetweennessCentrality(graph)
                .withDirection(configuration.getDirection(DEFAULT_DIRECTION))
                .compute();
        graph.release();
        return compute.resultStream();
    }

    @Procedure(value = "algo.betweenness", mode = Mode.WRITE)
    @Description("CALL algo.betweenness(label:String, relationship:String, {direction:'out',write:true, writeProperty:'centrality', stats:true, concurrency:4}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes, minCentrality, maxCentrality, sumCentrality - yields status of evaluation")
    public Stream<BetweennessCentralityProcResult> betweenness(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        if (configuration.getConcurrency() > 1) {
            return computeBetweennessParallel(label, relationship, configuration);
        } else {
            return computeBetweenness(label, relationship, configuration);
        }
    }


    /**
     * Randomized Approximate Brandes Algorithm
     * for approximating Betweenness Centrality
     *
     * optional Arguments:
     *  strategy:'degree'   Degree based randomization
     *  strategy:'random'   Randomized selection. Takes optional argument probability:double[0-1]
     *                      or use log10(nodeCount) / e^2 as default
     */
    @Procedure(value = "algo.betweenness.sampled", mode = Mode.WRITE)
    @Description("CALL algo.betweenness.sampled(label:String, relationship:String, {strategy:'random', probability:double, maxDepth:5, direction:'out',write:true, writeProperty:'centrality', stats:true, concurrency:4}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes, minCentrality, maxCentrality, sumCentrality - yields status of evaluation")
    public Stream<BetweennessCentralityProcResult> betweennessRABrandesWrite(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, label, relationship, configuration)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .withDirection(configuration.getDirection(Direction.OUTGOING))
                    .load(configuration.getGraphImpl());
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        final RABrandesBetweennessCentrality.SelectionStrategy strategy = strategy(configuration, graph);
        final RABrandesBetweennessCentrality bc =
                new RABrandesBetweennessCentrality(graph, Pools.DEFAULT, configuration.getConcurrency(), strategy)
                        .withProgressLogger(ProgressLogger.wrap(log, "Randomized Approximate Brandes: BetweennessCentrality(parallel)"))
                        .withTerminationFlag(terminationFlag)
                        .withDirection(configuration.getDirection(Direction.OUTGOING))
                        .withMaxDepth(configuration.getNumber("maxDepth", Integer.MAX_VALUE).intValue());

        builder.timeEval(() -> {
            bc.compute();
            if (configuration.isStatsFlag()) {
                computeStats(builder, bc.getCentrality());
                builder.withNodeCount(strategy.size());
            }
        });

        graph.release();
        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                final AtomicDoubleArray centrality = bc.getCentrality();
                final String writeProperty = configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY);
                Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build()
                        .write(writeProperty, centrality, Translators.ATOMIC_DOUBLE_ARRAY_TRANSLATOR);
            });
        }
        bc.release();

        return Stream.of(builder.build());
    }

    public Stream<BetweennessCentralityProcResult> computeBetweenness(
            String label,
            String relationship,
            ProcedureConfiguration configuration) {

        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, label, relationship, configuration)
                    .withoutNodeProperties()
                    .withDirection(configuration.getDirection(Direction.OUTGOING))
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(builder.build());
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        final BetweennessCentrality bc = new BetweennessCentrality(graph)
                .withTerminationFlag(terminationFlag)
                .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality(sequential)"))
                .withDirection(configuration.getDirection(Direction.OUTGOING));

        builder.timeEval(() -> {
            bc.compute();
            if (configuration.isStatsFlag()) {
                computeStats(builder, bc.getCentrality());
            }
        });

        final double[] centrality = bc.getCentrality();
        bc.release();
        graph.release();

        if (configuration.isWriteFlag()) {
            final String writeProperty = configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY);
            builder.timeWrite(() -> Exporter.of(api, graph)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                    .build()
                    .write(
                            writeProperty,
                            centrality,
                            Translators.DOUBLE_ARRAY_TRANSLATOR
                    )
            );
        }

        return Stream.of(builder.build());
    }

    public Stream<BetweennessCentralityProcResult> computeBetweennessParallel(
            String label,
            String relationship,
            ProcedureConfiguration configuration) {

        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, label, relationship, configuration)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .withDirection(configuration.getDirection(Direction.OUTGOING))
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(builder.build());
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        final ParallelBetweennessCentrality bc =
                new ParallelBetweennessCentrality(graph, Pools.DEFAULT, configuration.getConcurrency())
                        .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality(parallel)"))
                        .withTerminationFlag(terminationFlag)
                        .withDirection(configuration.getDirection(Direction.OUTGOING));

        builder.timeEval(() -> {
            bc.compute();
            if (configuration.isStatsFlag()) {
                computeStats(builder, bc.getCentrality());
            }
        });

        graph.release();
        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                final AtomicDoubleArray centrality = bc.getCentrality();
                final String writeProperty = configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY);
                Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                        .build()
                        .write(writeProperty, centrality, Translators.ATOMIC_DOUBLE_ARRAY_TRANSLATOR);
            });
        }
        bc.release();

        return Stream.of(builder.build());
    }

    private void computeStats(BetweennessCentralityProcResult.Builder builder, double[] centrality) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0.0;
        for (int i = centrality.length - 1; i >= 0; i--) {
            final double c = centrality[i];
            if (c < min) {
                min = c;
            }
            if (c > max) {
                max = c;
            }
            sum += c;
        }
        builder.withCentralityMax(max)
                .withCentralityMin(min)
                .withCentralitySum(sum);
    }

    private void computeStats(BetweennessCentralityProcResult.Builder builder, AtomicDoubleArray centrality) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0.0;
        for (int i = centrality.length() - 1; i >= 0; i--) {
            final double c = centrality.get(i);
            if (c < min) {
                min = c;
            }
            if (c > max) {
                max = c;
            }
            sum += c;
        }
        builder.withCentralityMax(max)
                .withCentralityMin(min)
                .withCentralitySum(sum);
    }

    private RABrandesBetweennessCentrality.SelectionStrategy strategy(ProcedureConfiguration configuration, Graph graph) {

        switch (configuration.getString("strategy", "random")) {

            case "degree":
                return new RandomDegreeSelectionStrategy(
                        configuration.getDirection(Direction.OUTGOING),
                        graph,
                        Pools.DEFAULT,
                        configuration.getConcurrency());

            default:
                final double probability = configuration.getNumber(
                        "probability",
                        Math.log10(graph.nodeCount()) / Math.exp(2)).doubleValue();
                return new RandomSelectionStrategy(
                        graph,
                        probability);
        }
    }
}
