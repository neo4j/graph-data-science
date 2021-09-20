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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.NodePropertiesWriter;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.impl.spanningTrees.KSpanningTree;
import org.neo4j.gds.impl.spanningTrees.Prim;
import org.neo4j.gds.impl.spanningTrees.SpanningTree;
import org.neo4j.gds.utils.InputNodeValidator;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class KSpanningTreeProc extends NodePropertiesWriter<KSpanningTree, SpanningTree, KSpanningTreeConfig> {

    private static final String MAX_DESCRIPTION =
        "The maximum weight spanning tree (MST) starts from a given node, and finds all its reachable nodes " +
        "and the set of relationships that connect the nodes together with the maximum possible weight.";

    private static final String MIN_DESCRIPTION =
        "The minimum weight spanning tree (MST) starts from a given node, and finds all its reachable nodes " +
        "and the set of relationships that connect the nodes together with the minimum possible weight.";

    static DoubleUnaryOperator minMax;

    public static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Procedure(value = "gds.alpha.spanningTree.kmax.write", mode = WRITE)
    @Description(MAX_DESCRIPTION)
    public Stream<Prim.Result> kmax(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        minMax = Prim.MAX_OPERATOR;
        return computeAndWrite(graphNameOrConfig, configuration);
    }

    @Procedure(value = "gds.alpha.spanningTree.kmin.write", mode = WRITE)
    @Description(MIN_DESCRIPTION)
    public Stream<Prim.Result> kmin(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        minMax = Prim.MIN_OPERATOR;
        return computeAndWrite(graphNameOrConfig, configuration);
    }

    public Stream<Prim.Result> computeAndWrite(Object graphNameOrConfig, Map<String, Object> configuration) {
        ComputationResult<KSpanningTree, SpanningTree, KSpanningTreeConfig> computationResult = compute(graphNameOrConfig, configuration);

        Graph graph = computationResult.graph();
        SpanningTree spanningTree = computationResult.result();
        KSpanningTreeConfig config = computationResult.config();

        Prim.Builder builder = new Prim.Builder();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        builder.withEffectiveNodeCount(spanningTree.effectiveNodeCount);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
            var task = Tasks.leaf("KSpanningTree :: WriteNodeProperties", graph.nodeCount());
            var progressTracker = new TaskProgressTracker(task, log, config.writeConcurrency(), taskRegistryFactory);
            final NodePropertyExporter exporter = nodePropertyExporterBuilder
                .withIdMapping(graph)
                .withTerminationFlag(TerminationFlag.wrap(transaction)).withProgressTracker(progressTracker)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build();

            var properties = new DoubleNodeProperties() {
                @Override
                public long size() {
                    return computationResult.graph().nodeCount();
                }

                @Override
                public double doubleValue(long nodeId) {
                    return spanningTree.head((int) nodeId);
                }
            };

            progressTracker.beginSubTask();
            exporter.write(
                config.writeProperty(),
                properties
            );
            progressTracker.endSubTask();

            builder.withNodePropertiesWritten(exporter.propertiesWritten());
        }

        builder.withComputeMillis(computationResult.computeMillis());
        builder.withCreateMillis(computationResult.createMillis());
        return Stream.of(builder.build());
    }

    @Override
    protected KSpanningTreeConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return KSpanningTreeConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<KSpanningTree, KSpanningTreeConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            protected String taskName() {
                return "KSpanningTree";
            }

            @Override
            protected Task progressTask(
                Graph graph, KSpanningTreeConfig config
            ) {
                return Tasks.task(
                    taskName(),
                    Tasks.leaf("SpanningTree", graph.nodeCount()),
                    Tasks.leaf("Add relationship weights"),
                    Tasks.leaf("Remove relationships")
                );
            }

            @Override
            protected KSpanningTree build(
                Graph graph,
                KSpanningTreeConfig configuration,
                AllocationTracker allocationTracker,
                ProgressTracker progressTracker
            ) {
                InputNodeValidator.validateStartNode(configuration.startNodeId(), graph);
                return new KSpanningTree(graph, graph, graph, minMax, configuration.startNodeId(), configuration.k(), progressTracker);
            }
        };
    }
}
