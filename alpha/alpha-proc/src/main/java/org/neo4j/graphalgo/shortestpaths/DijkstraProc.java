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

import com.carrotsearch.hppc.IntArrayDeque;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.shortestpaths.DijkstraConfig;
import org.neo4j.graphalgo.impl.shortestpaths.ShortestPathDijkstra;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class DijkstraProc extends AlgoBaseProc<ShortestPathDijkstra, ShortestPathDijkstra, DijkstraConfig> {

    private static final String DESCRIPTION =
        "The Shortest Path algorithm calculates the shortest (weighted) " +
        "path between a pair of nodes using the Dijkstra algorithm.";

    /**
     * single threaded dijkstra impl.
     * takes a startNode and endNode and tries to find the shortest path
     * supports direction flag in configuration ( see {@link org.neo4j.graphalgo.core.utils.Directions})
     * default is: BOTH
     */
    @Procedure(name = "gds.alpha.shortestPath.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ShortestPathDijkstra.Result> dijkstraStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ShortestPathDijkstra, ShortestPathDijkstra, DijkstraConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return computationResult.algorithm().resultStream();
    }

    @Procedure(value = "gds.alpha.shortestPath.write", mode = Mode.WRITE)
    @Description(DESCRIPTION)
    public Stream<DijkstraResult> dijkstraWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ComputationResult<ShortestPathDijkstra, ShortestPathDijkstra, DijkstraConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        DijkstraResult.Builder builder = DijkstraResult.builder();
        builder.setCreateMillis(computationResult.createMillis());
        builder.setComputeMillis(computationResult.computeMillis());

        Graph graph = computationResult.graph();
        ShortestPathDijkstra dijkstra = computationResult.algorithm();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        builder.withNodeCount(dijkstra.getPathLength())
               .withTotalCosts(dijkstra.getTotalCost());

        try (ProgressTimer timer = builder.timeWrite()) {
            final IntArrayDeque finalPath = dijkstra.getFinalPath();
            final double[] finalPathCost = dijkstra.getFinalPathCosts();
            dijkstra.release();

            DequeMapping mapping = new DequeMapping(graph, finalPath);
            NodePropertyExporter.of(api, mapping, dijkstra.getTerminationFlag())
                .withLog(log)
                .build()
                .write(
                    computationResult.config().writeProperty(),
                    finalPathCost,
                    Translators.DOUBLE_ARRAY_TRANSLATOR
                );
        }

        graph.release();
        return Stream.of(builder.build());
    }

    @Override
    protected DijkstraConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return DijkstraConfig.of(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    protected AlgorithmFactory<ShortestPathDijkstra, DijkstraConfig> algorithmFactory(DijkstraConfig config) {
        return new AlphaAlgorithmFactory<ShortestPathDijkstra, DijkstraConfig>() {
            @Override
            public ShortestPathDijkstra build(
                Graph graph,
                DijkstraConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new ShortestPathDijkstra(graph, configuration);
            }
        };
    }

    private static final class DequeMapping implements IdMapping {
        private final IdMapping mapping;
        private final int[] data;
        private final int offset;
        private final int length;

        private DequeMapping(IdMapping mapping, IntArrayDeque data) {
            this.mapping = mapping;
            if (data.head <= data.tail) {
                this.data = data.buffer;
                this.offset = data.head;
                this.length = data.tail - data.head;
            } else {
                this.data = data.toArray();
                this.offset = 0;
                this.length = this.data.length;
            }
        }

        @Override
        public long toMappedNodeId(final long nodeId) {
            return mapping.toMappedNodeId(nodeId);
        }

        @Override
        public long toOriginalNodeId(final long nodeId) {
            assert nodeId < length;
            return mapping.toOriginalNodeId(data[offset + Math.toIntExact(nodeId)]);
        }

        @Override
        public boolean contains(final long nodeId) {
            return true;
        }

        @Override
        public long nodeCount() {
            return length;
        }
    }

    public static class DijkstraResult {

        public final long createMillis;
        public final long evalMillis;
        public final long writeMillis;
        public final long nodeCount;
        public final double totalCost;

        public DijkstraResult(long createMillis, long evalMillis, long writeMillis, long nodeCount, double totalCost) {
            this.createMillis = createMillis;
            this.evalMillis = evalMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
            this.totalCost = totalCost;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder extends AbstractResultBuilder<DijkstraResult> {

            protected long nodeCount = 0;
            protected double totalCosts = 0.0;

            public Builder withNodeCount(long nodeCount) {
                this.nodeCount = nodeCount;
                return this;
            }

            public Builder withTotalCosts(double totalCosts) {
                this.totalCosts = totalCosts;
                return this;
            }

            public DijkstraResult build() {
                return new DijkstraResult(createMillis, computeMillis, writeMillis, nodeCount, totalCosts);
            }
        }
    }
}
