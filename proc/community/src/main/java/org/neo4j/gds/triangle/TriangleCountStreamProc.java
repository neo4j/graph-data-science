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
package org.neo4j.gds.triangle;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.triangleCount.stream", description = TriangleCountCompanion.DESCRIPTION, executionMode = STREAM)
public class TriangleCountStreamProc
    extends StreamProc<IntersectingTriangleCount, TriangleCountResult,
    TriangleCountStreamProc.Result, TriangleCountStreamConfig> {

    @Description(TriangleCountCompanion.DESCRIPTION)
    @Procedure(name = "gds.triangleCount.stream", mode = Mode.READ)
    public Stream<Result> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphName, configuration));
    }

    @Procedure(value = "gds.triangleCount.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    protected Stream<Result> stream(ComputationResult<IntersectingTriangleCount, TriangleCountResult, TriangleCountStreamConfig> computationResult) {
        return runWithExceptionLogging("Graph streaming failed", () -> {
            var graph = computationResult.graph();
            var result = computationResult.result();

            return LongStream.range(0, graph.nodeCount())
                .mapToObj(i -> new Result(
                    graph.toOriginalNodeId(i),
                    result.localTriangles().get(i)
                ));
        });
    }

    @Override
    protected Result streamResult(
        long originalNodeId, long internalNodeId, NodePropertyValues nodePropertyValues
    ) {
        throw new UnsupportedOperationException("TriangleCount handles result building individually.");
    }

    @Override
    protected TriangleCountStreamConfig newConfig(String username, CypherMapWrapper config) {
        return TriangleCountStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<IntersectingTriangleCount, TriangleCountStreamConfig> algorithmFactory() {
        return new IntersectingTriangleCountFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(
        ComputationResult<IntersectingTriangleCount, TriangleCountResult, TriangleCountStreamConfig> computationResult
    ) {
        return TriangleCountCompanion.nodePropertyTranslator(computationResult);
    }

    @SuppressWarnings("unused")
    public static class Result {

        public final long nodeId;
        public final long triangleCount;

        public Result(long nodeId, long triangleCount) {
            this.nodeId = nodeId;
            this.triangleCount = triangleCount;
        }
    }
}
