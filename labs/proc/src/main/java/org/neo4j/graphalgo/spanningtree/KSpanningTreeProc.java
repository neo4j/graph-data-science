/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.spanningtree;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.spanningTrees.KSpanningTree;
import org.neo4j.graphalgo.impl.spanningTrees.Prim;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Stream;

public class KSpanningTreeProc extends AlgoBaseProc<KSpanningTree, SpanningTree, KSpanningTreeConfig> {

    static DoubleUnaryOperator minMax;

    public static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Procedure(value = "gds.alpha.spanningTree.kmax.write", mode = Mode.WRITE)
    public Stream<Prim.Result> kmax(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        minMax = Prim.MAX_OPERATOR;
        return computeAndWrite(graphNameOrConfig, configuration);
    }

    @Procedure(value = "gds.alpha.spanningTree.kmin.write", mode = Mode.WRITE)
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
        builder.timeWrite(() -> {
            final NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, TerminationFlag.wrap(transaction))
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build();

            exporter.write(
                config.writeProperty(),
                spanningTree,
                SpanningTree.TRANSLATOR);
        });
        builder.setComputeMillis(computationResult.computeMillis());
        builder.setLoadMillis(computationResult.createMillis());
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
    protected AlgorithmFactory<KSpanningTree, KSpanningTreeConfig> algorithmFactory(KSpanningTreeConfig config) {
        return new AlphaAlgorithmFactory<KSpanningTree, KSpanningTreeConfig>() {
            @Override
            public KSpanningTree build(
                Graph graph,
                KSpanningTreeConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new KSpanningTree(graph, graph, graph, minMax, configuration.startNodeId(), configuration.k());
            }
        };
    }
}
