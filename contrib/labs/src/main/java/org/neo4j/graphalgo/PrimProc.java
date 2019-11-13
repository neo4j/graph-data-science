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
import org.neo4j.graphalgo.core.write.RelationshipExporter;
import org.neo4j.graphalgo.impl.spanningTrees.Prim;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningGraph;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class PrimProc extends LabsProc {

    public static final String CONFIG_WRITE_RELATIONSHIP = "writeProperty";
    public static final String CONFIG_WRITE_RELATIONSHIP_DEFAULT = "MST";

    @Procedure(value = "algo.mst", mode = Mode.WRITE)
    @Description("CALL algo.mst(label:String, relationshipType:String, weightProperty:String, startNodeId:long, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> deprecatedProc(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return minimumSpanningTree(label, relationship, weightProperty, startNode, config);
    }

    @Procedure(value = "algo.spanningTree", mode = Mode.WRITE)
    @Description("CALL algo.spanningTree(label:String, relationshipType:String, weightProperty:String, startNodeId:long, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> defaultProc(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return spanningTree(label, relationship, weightProperty, startNode, config, false);
    }

    @Procedure(value = "algo.spanningTree.minimum", mode = Mode.WRITE)
    @Description("CALL algo.spanningTree.minimum(label:String, relationshipType:String, weightProperty:String, startNodeId:long, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> minimumSpanningTree(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return spanningTree(label, relationship, weightProperty, startNode, config, false);
    }

    @Procedure(value = "algo.spanningTree.maximum", mode = Mode.WRITE)
    @Description("CALL algo.spanningTree.maximum(label:String, relationshipType:String, weightProperty:String, startNodeId:long, {" +
            "writeProperty:String}) " +
            "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount")
    public Stream<Prim.Result> maximumSpanningTree(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return spanningTree(label, relationship, weightProperty, startNode, config, true);
    }

    public Stream<Prim.Result> spanningTree(String label,
                                            String relationship,
                                            String weightProperty,
                                            long startNode,
                                            Map<String, Object> config,
                                            boolean max) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        final Prim.Builder builder = new Prim.Builder();
        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withRelationshipProperties(PropertyMapping.of(weightProperty, configuration.getWeightPropertyDefaultValue(Double.MAX_VALUE)))
                    .undirected()
                    .withLog(log)
                    .load(configuration.getGraphImpl(Graph.TYPE));
        }

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        final int root = Math.toIntExact(graph.toMappedNodeId(startNode));
        final Prim mstPrim = new Prim(graph, graph)
                .withProgressLogger(ProgressLogger.wrap(log, "Prim(MaximumSpanningTree)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));
        builder.timeEval(() -> {
            if (max) {
                mstPrim.computeMaximumSpanningTree(root);
            } else {
                mstPrim.computeMinimumSpanningTree(root);
            }
        });

        final SpanningTree spanningTree = mstPrim.getSpanningTree();
        SpanningGraph spanningGraph = new SpanningGraph(graph, spanningTree);
        builder.withEffectiveNodeCount(spanningTree.effectiveNodeCount);
        if (configuration.isWriteFlag()) {
            mstPrim.release();
            builder.timeWrite(() -> {
                RelationshipExporter.of(
                    api,
                    graph,
                    graph,
                    spanningGraph,
                    spanningGraph,
                    spanningTree.effectiveNodeCount
                )
                        .withLog(log)
                        .build()
                        .write(
                            configuration.get(CONFIG_WRITE_RELATIONSHIP, CONFIG_WRITE_RELATIONSHIP_DEFAULT),
                            weightProperty,
                            0.0,
                            Direction.OUTGOING);
            });
        }
        return Stream.of(builder.build());
    }
}
