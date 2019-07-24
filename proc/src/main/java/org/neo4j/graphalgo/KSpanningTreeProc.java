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
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.spanningTrees.KSpanningTree;
import org.neo4j.graphalgo.impl.spanningTrees.Prim;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class KSpanningTreeProc {

    private static final String CONFIG_CLUSTER_PROPERTY = "writeProperty";
    private static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    @Procedure(value = "algo.spanningTree.kmax", mode = Mode.WRITE)
    @Description("CALL algo.spanningTree.kmax(label:String, relationshipType:String, weightProperty:String, startNodeId:long, k:int, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> kmax(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "k") long k,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return spanningTree(label, relationship, weightProperty, startNode, k, config, true);
    }

    @Procedure(value = "algo.spanningTree.kmin", mode = Mode.WRITE)
    @Description("CALL algo.spanningTree.kmin(label:String, relationshipType:String, weightProperty:String, startNodeId:long, k:int, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> kmin(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "k") long k,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return spanningTree(label, relationship, weightProperty, startNode, k, config, false);
    }

    public Stream<Prim.Result> spanningTree(String label,
                                            String relationship,
                                            String weightProperty,
                                            long startNode,
                                            long k,
                                            Map<String, Object> config,
                                            boolean max) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final Prim.Builder builder = new Prim.Builder();
        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withRelationshipWeightsFromProperty(weightProperty, configuration.getWeightPropertyDefaultValue(1.0))
                    .withoutNodeWeights()
                    .withLog(log)
                    .undirected()
                    .load(configuration.getGraphImpl());
        }

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.withEffectiveNodeCount(0).build());
        }

        final long root = graph.toMappedNodeId(startNode);

        final KSpanningTree kSpanningTree = new KSpanningTree(graph, graph, graph)
                .withProgressLogger(ProgressLogger.wrap(log, "KSpanningTrees"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(() -> {
            kSpanningTree.compute(root, k, max);
            builder.withEffectiveNodeCount(kSpanningTree.getSpanningTree().effectiveNodeCount);
        });

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {

                final SpanningTree spanningTree = kSpanningTree.getSpanningTree();
                final Exporter exporter = Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(
                                Pools.DEFAULT,
                                configuration.getWriteConcurrency(),
                                TerminationFlag.wrap(transaction))
                        .build();

                exporter.write(
                        configuration.get(
                                CONFIG_CLUSTER_PROPERTY,
                                DEFAULT_CLUSTER_PROPERTY),
                        spanningTree,
                        SpanningTree.TRANSLATOR);
            }
        }

        return Stream.of(builder.build());
    }
}
