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
package org.neo4j.graphalgo.shortestpaths;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.FilterGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.shortestpaths.WeightedPathExporter;
import org.neo4j.graphalgo.impl.shortestpaths.YensKShortestPaths;
import org.neo4j.graphalgo.impl.shortestpaths.YensKShortestPathsConfig;
import org.neo4j.graphalgo.impl.shortestpaths.YensKShortestPathsConfigImpl;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Path;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

/**
 * Yen's K shortest paths algorithm. Computes multiple shortest
 * paths from a given start to a goal node in the desired direction.
 * The paths are written to the graph using new relationships named
 * by prefix + index.
 */
public class KShortestPathsProc extends AlgoBaseProc<YensKShortestPaths, YensKShortestPaths, YensKShortestPathsConfig> {

    @Override
    protected boolean legacyMode() {
        return false;
    }

    @Procedure(name = "gds.alpha.kShortestPaths.stream", mode = READ)
    public Stream<KspStreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ComputationResult<YensKShortestPaths, YensKShortestPaths, YensKShortestPathsConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        KspResult.Builder builder = new KspResult.Builder();
        builder.setLoadMillis(computationResult.createMillis());
        builder.setComputeMillis(computationResult.computeMillis());

        Graph graph = computationResult.graph();
        YensKShortestPaths algorithm = computationResult.algorithm();
        YensKShortestPathsConfig config = computationResult.config();

        if (graph.isEmpty() || algorithm == null) {
            graph.release();
            return Stream.empty();
        }

        builder.withResultCount(algorithm.getPaths().size());
        boolean returnPath = config.path();

        Pointer.IntPointer counter = Pointer.wrap(0);
        return algorithm.getPaths().stream().map(weightedPath -> {
            long[] nodeIds = new long[weightedPath.size()];
            AtomicInteger count = new AtomicInteger(0);
            weightedPath.forEach(i -> {
                long originalNodeId = graph.toOriginalNodeId(i);
                nodeIds[count.getAndIncrement()] = originalNodeId;
                return true;
            });

            count.set(0);

            double[] costs = new double[weightedPath.size() - 1];
            weightedPath.forEachEdge((sourceNode, targetNode) -> {
                double cost = graph.relationshipProperty(sourceNode, targetNode, 1.0D);
                costs[count.getAndIncrement()] = cost;
            });

            Path path = null;
            if (returnPath) {
                if (config.relationshipWeightProperty() != null) {
                    path = WalkPath.toPath(api, nodeIds, costs);
                } else {
                    path = WalkPath.toPath(api, nodeIds);
                }
            }

            return new KspStreamResult(counter.v++, nodeIds, path, costs);
        });
    }

    @Override
    protected Pair<YensKShortestPathsConfig, Optional<String>> processInput(
        Object graphNameOrConfig, Map<String, Object> configuration
    ) {
        return super.processInput(graphNameOrConfig, configuration);
    }

    @Procedure(value = "gds.alpha.kShortestPaths.write", mode = Mode.WRITE)
    public Stream<KspResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ComputationResult<YensKShortestPaths, YensKShortestPaths, YensKShortestPathsConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        KspResult.Builder builder = new KspResult.Builder();
        builder.setLoadMillis(computationResult.createMillis());
        builder.setComputeMillis(computationResult.computeMillis());

        Graph graph = computationResult.graph();
        YensKShortestPaths algorithm = computationResult.algorithm();
        YensKShortestPathsConfig config = computationResult.config();

        if (graph.isEmpty() || algorithm == null) {
            ReleaseBlockedGraph.runRelease(graph);
            return Stream.of(builder.build());
        }

        builder.withResultCount(algorithm.getPaths().size());
        try (ProgressTimer timer = builder.timeWrite()) {
            new WeightedPathExporter(
                api,
                Pools.DEFAULT,
                graph,
                graph,
                config.writePropertyPrefix(),
                config.relationshipWriteProperty()
            ).export(algorithm.getPaths());
        }

        ReleaseBlockedGraph.runRelease(graph);
        return Stream.of(builder.build());
    }

    @Override
    protected YensKShortestPathsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return new YensKShortestPathsConfigImpl(graphName, maybeImplicitCreate, username, config);
    }

    @Override
    protected Graph createGraph(Pair<YensKShortestPathsConfig, Optional<String>> configAndName) {
        Graph graph = super.createGraph(configAndName);
        return new ReleaseBlockedGraph(graph);
    }

    @Override
    protected AlgorithmFactory<YensKShortestPaths, YensKShortestPathsConfig> algorithmFactory(YensKShortestPathsConfig config) {
        return new AlphaAlgorithmFactory<YensKShortestPaths, YensKShortestPathsConfig>() {
            @Override
            public YensKShortestPaths build(
                Graph graph,
                YensKShortestPathsConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new YensKShortestPaths(
                    graph,
                    configuration.startNode(),
                    configuration.endNode(),
                    configuration.k(),
                    configuration.maxDepth()
                );
            }
        };
    }

    public static class KspStreamResult {
        public Long index;
        public Long sourceNodeId;
        public Long targetNodeId;
        public List<Long> nodeIds;
        public List<Double> costs;
        public Path path;

        public KspStreamResult(long index, long[] nodes, Path path, double[] costs) {
            this.index = index;
            this.sourceNodeId = nodes.length > 0 ? nodes[0] : null;
            this.targetNodeId = nodes.length > 0 ? nodes[nodes.length - 1] : null;

            this.nodeIds = new ArrayList<>(nodes.length);
            for (long node : nodes) this.nodeIds.add(node);

            this.costs = new ArrayList<>(costs.length);
            for (double cost : costs) this.costs.add(cost);

            this.path = path;
        }
    }

    public static class KspResult {

        public final long loadMillis;
        public final long evalMillis;
        public final long writeMillis;
        public final long resultCount;

        public KspResult(long loadMillis, long evalMillis, long writeMillis, long resultCount) {
            this.loadMillis = loadMillis;
            this.evalMillis = evalMillis;
            this.writeMillis = writeMillis;
            this.resultCount = resultCount;
        }

        public static class Builder extends AbstractResultBuilder<KspResult> {

            private int resultCount;

            public Builder withResultCount(int resultCount) {
                this.resultCount = resultCount;
                return this;
            }

            @Override
            public KspResult build() {
                return new KspResult(
                    loadMillis,
                    computeMillis,
                    writeMillis,
                    resultCount
                );
            }
        }
    }

    private static final class ReleaseBlockedGraph extends FilterGraph {

        static void runRelease(Graph graph) {
            if (graph instanceof ReleaseBlockedGraph) {
                ((ReleaseBlockedGraph) graph).actuallyRelease();
            } else {
                graph.release();
            }
        }

        public ReleaseBlockedGraph(Graph graph) {
            super(graph);
        }

        @Override
        public void release() {
        }

        void actuallyRelease() {
            super.release();
        }
    }
}
