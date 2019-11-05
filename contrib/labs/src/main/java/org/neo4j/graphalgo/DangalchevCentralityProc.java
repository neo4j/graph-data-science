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
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.DangalchevClosenessCentrality;
import org.neo4j.graphalgo.results.CentralityProcResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class DangalchevCentralityProc extends LabsProc {

    public static final String DEFAULT_TARGET_PROPERTY = "centrality";

    @Procedure(name = "algo.closeness.dangalchev.stream", mode = READ)
    @Description("CALL algo.closeness.dangalchev.stream(label:String, relationship:String{concurrency:4}) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<DangalchevClosenessCentrality.Result> dangalchevStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername())
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        final AllocationTracker tracker = AllocationTracker.create();

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                .withAllocationTracker(tracker)
                .undirected()
                .load(configuration.getGraphImpl("huge"));

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        final DangalchevClosenessCentrality algo = new DangalchevClosenessCentrality(graph, configuration.getConcurrency(), Pools.DEFAULT)
                .withProgressLogger(ProgressLogger.wrap(log, "DangalchevCentrality"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();

        graph.release();

        return algo.resultStream();
    }

    @Procedure(value = "algo.closeness.dangalchev", mode = Mode.WRITE)
    @Description("CALL algo.closeness.dangalchev(label:String, relationship:String, {write:true, writeProperty:'centrality, concurrency:4'}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes] - yields evaluation details")
    public Stream<CentralityProcResult> dangalchev(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername())
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        final CentralityProcResult.Builder builder = CentralityProcResult.builder();

        final AllocationTracker tracker = AllocationTracker.create();
        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                    .withAllocationTracker(tracker)
                    .undirected()
                    .load(configuration.getGraphImpl("huge"));
        }

        builder.withNodeCount(graph.nodeCount());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        final DangalchevClosenessCentrality algo = new DangalchevClosenessCentrality(graph, configuration.getConcurrency(), Pools.DEFAULT)
                .withProgressLogger(ProgressLogger.wrap(log, "DangalchevCentrality"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(algo::compute);

        if (configuration.isWriteFlag()) {
            graph.release();
            final String writeProperty = configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY);
            builder.timeWrite(() -> {
                Exporter exporter = Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getWriteConcurrency(), terminationFlag)
                        .build();
                algo.export(writeProperty, exporter);
            });
            algo.release();
        }

        return Stream.of(builder.build());
    }
}
