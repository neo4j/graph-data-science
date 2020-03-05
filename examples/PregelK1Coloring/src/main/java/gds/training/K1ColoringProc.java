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
package gds.training;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class K1ColoringProc extends AlgoBaseProc<K1ColoringAlgorithm, HugeDoubleArray, K1ColoringPregelConfig> {
    static final String DESCRIPTION = "The K-1 Coloring algorithm assigns a color to every node in the graph.";


    @Description(DESCRIPTION)
    @Procedure(value = "gds.beta.k1coloring.pregel")
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<K1ColoringAlgorithm, HugeDoubleArray, K1ColoringPregelConfig> compute = compute(graphNameOrConfig, configuration);

        return Optional.ofNullable(compute.result())
            .map(coloring -> {
                Graph graph = compute.graph();
                return LongStream.range(0, graph.nodeCount())
                    .mapToObj(nodeId -> {
                        long neoNodeId = graph.toOriginalNodeId(nodeId);
                        return new StreamResult(neoNodeId, Double.valueOf(coloring.get(nodeId)).longValue());
                    });

            }).orElse(Stream.empty());
    }

    @Override
    protected K1ColoringPregelConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return K1ColoringPregelConfig.of(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    protected AlgorithmFactory<K1ColoringAlgorithm, K1ColoringPregelConfig> algorithmFactory(K1ColoringPregelConfig config) {
        return new AlgorithmFactory<K1ColoringAlgorithm, K1ColoringPregelConfig>() {

            @Override
            public K1ColoringAlgorithm build(
                    Graph graph,
                    K1ColoringPregelConfig configuration,
                    AllocationTracker tracker,
                    Log log) {
                return new K1ColoringAlgorithm(graph, configuration.maxIterations());
            }

            @Override
            public MemoryEstimation memoryEstimation(K1ColoringPregelConfig configuration) {
                throw new UnsupportedOperationException(".memoryEstimation is not implemented.");
            }
        };
    }

    public static class StreamResult {
        public final long nodeId;
        public final long color;

        StreamResult(long nodeId, long color) {
            this.nodeId = nodeId;
            this.color = color;
        }
    }
}
