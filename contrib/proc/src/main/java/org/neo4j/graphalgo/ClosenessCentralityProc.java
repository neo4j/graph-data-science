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
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.closeness.HugeMSClosenessCentrality;
import org.neo4j.graphalgo.impl.closeness.MSBFSCCAlgorithm;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;
import org.neo4j.graphalgo.results.CentralityProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ClosenessCentralityProc {


    public static final String DEFAULT_TARGET_PROPERTY = "centrality";


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.closeness.stream")
    @Description("CALL algo.closeness.stream(label:String, relationship:String{concurrency:4}) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<MSClosenessCentrality.Result> closenessStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        AllocationTracker tracker = AllocationTracker.create();

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withoutNodeProperties()
                .asUndirected(true)
                .withDirection(Direction.OUTGOING)
                .withAllocationTracker(tracker)
                .load(configuration.getGraphImpl());

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        final MSBFSCCAlgorithm<?> algo = newAlgo(tracker,
                graph,
                configuration.getConcurrency(),
                configuration.get("improved", Boolean.FALSE));
        algo
                .withProgressLogger(ProgressLogger.wrap(log, "ClosenessCentrality(MultiSource)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
        algo.compute();
        graph.release();
        return algo.resultStream();
    }

    private MSBFSCCAlgorithm<?> newAlgo(
            final AllocationTracker tracker,
            final Graph graph,
            final int concurrency,
            final boolean wassermanFaust) {
        final MSBFSCCAlgorithm<?> algo;
        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            algo = new HugeMSClosenessCentrality(
                    hugeGraph,
                    tracker,
                    concurrency,
                    Pools.DEFAULT, wassermanFaust);
        } else {
            algo = new MSClosenessCentrality(
                    graph,
                    concurrency,
                    Pools.DEFAULT, wassermanFaust);
        }
        return algo;
    }

    @Procedure(value = "algo.closeness", mode = Mode.WRITE)
    @Description("CALL algo.closeness(label:String, relationship:String, {write:true, writeProperty:'centrality, concurrency:4'}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes] - yields evaluation details")
    public Stream<CentralityProcResult> closeness(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final CentralityProcResult.Builder builder = CentralityProcResult.builder();

        AllocationTracker tracker = AllocationTracker.create();
        int concurrency = configuration.getConcurrency();
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, label, relationship, configuration)
                    .withoutNodeProperties()
                    .withDirection(Direction.OUTGOING)
                    .withAllocationTracker(tracker)
                    .asUndirected(true)
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(builder.build());
        }

        final MSBFSCCAlgorithm<?> algo = newAlgo(tracker,
                graph,
                concurrency,
                configuration.get("improved", Boolean.FALSE));
        algo
                .withProgressLogger(ProgressLogger.wrap(log, "ClosenessCentrality(MultiSource)"))
                .withTerminationFlag(terminationFlag);

        builder.timeEval(algo::compute);

        if (configuration.isWriteFlag()) {
            graph.release();
            final String writeProperty = configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY);
            builder.timeWrite(() -> {
                Exporter exporter = Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, concurrency, terminationFlag)
                        .build();
                algo.export(writeProperty, exporter);
            });
            algo.release();
        }

        return Stream.of(builder.build());
    }
}
