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

package org.neo4j.graphalgo.triangle;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.triangle.IntersectingTriangleCount.TriangleCountResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

@Deprecated
public class AlphaTriangleCountStreamProc extends TriangleBaseProc<TriangleCountStreamConfig> {

    @Procedure(name = "gds.alpha.triangleCount.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<IntersectingTriangleCount, TriangleCountResult, TriangleCountStreamConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        Graph graph = computationResult.graph();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        return IntStream.range(0, Math.toIntExact(graph.nodeCount()))
            .mapToObj(i -> new Result(
                graph.toOriginalNodeId(i),
                computationResult.result().localTriangles().get(i),
                computationResult.result().localClusteringCoefficients().get(i)
            ));
    }

    @Override
    protected TriangleCountStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return TriangleCountStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }


    /**
     * result type
     */
    public static class Result {

        public final long nodeId;
        public final long triangles;
        public final double coefficient;

        public Result(long nodeId, long triangles, double coefficient) {
            this.nodeId = nodeId;
            this.triangles = triangles;
            this.coefficient = coefficient;
        }
        @Override
        public String toString() {
            return "Result{" +
                   "nodeId=" + nodeId +
                   ", triangles=" + triangles +
                   ", coefficient=" + coefficient +
                   '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result result = (Result) o;
            return nodeId == result.nodeId &&
                   triangles == result.triangles &&
                   Double.compare(result.coefficient, coefficient) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, triangles, coefficient);
        }
    }
}
