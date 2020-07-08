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
package gds.example;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class K1ColoringProc extends StreamProc<K1ColoringAlgorithm, HugeDoubleArray, K1ColoringProc.StreamResult, K1ColoringPregelConfig> {

    @Description("The K-1 Coloring algorithm assigns a color to every node in the graph.")
    @Procedure(value = "gds.example.k1coloring.pregel")
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Override
    protected StreamResult streamResult(long originalNodeId, double value) {
        return new StreamResult(originalNodeId, (long) value);
    }

    @Override
    protected PropertyTranslator<HugeDoubleArray> nodePropertyTranslator(ComputationResult<K1ColoringAlgorithm, HugeDoubleArray, K1ColoringPregelConfig> computationResult) {
        return HugeDoubleArray.Translator.INSTANCE;
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
        return new AlgorithmFactory<>() {

            @Override
            public K1ColoringAlgorithm build(
                    Graph graph,
                    K1ColoringPregelConfig configuration,
                    AllocationTracker tracker,
                    Log log
            ) {
                return new K1ColoringAlgorithm(graph, configuration.maxIterations());
            }

            @Override
            public MemoryEstimation memoryEstimation(K1ColoringPregelConfig configuration) {
                throw new MemoryEstimationNotImplementedException();
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
