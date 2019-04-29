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
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.MSColoring;
import org.neo4j.graphalgo.impl.UnionFindProcExec;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class MSColoringProc {

    public static final String CONFIG_CLUSTER_PROPERTY = "partitionProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.unionFind.mscoloring", mode = Mode.WRITE)
    @Description("CALL algo.unionFind.mscoloring(label:String, relationship:String, " +
            "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition', concurrency:4}) " +
            "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindProcExec.UnionFindResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final UnionFindProcExec.Builder builder = new UnionFindProcExec.Builder();

        // loading
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = load(configuration, tracker);
        }

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(UnionFindProcExec.UnionFindResult.EMPTY);
        }

        // evaluation
        final AtomicIntegerArray struct;
        try (ProgressTimer timer = builder.timeEval()) {
            struct = evaluate(graph, configuration);
        }

        if (configuration.isWriteFlag()) {
            // write back
            builder.timeWrite(() ->
                    write(graph, struct, configuration));
        }

        return Stream.of(builder.build(tracker, graph.nodeCount(), n -> (long) struct.get((int) n)));
    }

    @Procedure(value = "algo.unionFind.mscoloring.stream")
    @Description("CALL algo.unionFind.mscoloring.stream(label:String, relationship:String, " +
            "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4) " +
            "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<MSColoring.Result> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        // loading
        AllocationTracker tracker = AllocationTracker.create();
        final Graph graph = load(configuration, tracker);

        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }


        // evaluation
        return new MSColoring(graph, Pools.DEFAULT, configuration.getConcurrency())
                .compute()
                .resultStream();
    }

    private Graph load(
            ProcedureConfiguration config,
            AllocationTracker tracker) {
        return new GraphLoader(api, Pools.DEFAULT)
                .init(log, config.getNodeLabelOrQuery(),config.getRelationshipOrQuery(),config)
                .withAllocationTracker(tracker)
                .withOptionalRelationshipWeightsFromProperty(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .load(config.getGraphImpl());
    }

    private AtomicIntegerArray evaluate(Graph graph, ProcedureConfiguration config) {
        return new MSColoring(graph, Pools.DEFAULT, config.getConcurrency())
                .compute()
                .getColors();
    }

    private void write(Graph graph, AtomicIntegerArray struct, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        Exporter.of(api, graph)
                .withLog(log)
                .parallel(Pools.DEFAULT, configuration.getConcurrency(), null)
                .build()
                .write(
                        configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                        struct,
                        Translators.ATOMIC_INTEGER_ARRAY_TRANSLATOR
                );
    }
}
