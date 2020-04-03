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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.impl.triangle.IntersectingTriangleCount;
import org.neo4j.graphalgo.impl.triangle.TriangleCountStreamConfig;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class TriangleCountStatsProc extends TriangleBaseProc<TriangleCountStreamConfig> {

    @Procedure(value = "gds.alpha.triangleCount.stats", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<Result> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<IntersectingTriangleCount, PagedAtomicIntegerArray, TriangleCountStreamConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        TriangleCountStreamConfig config = computationResult.config();
        Graph graph = computationResult.graph();
        IntersectingTriangleCount algorithm = computationResult.algorithm();

        TriangleCountResultBuilder builder = new TriangleCountResultBuilder(callContext, computationResult.tracker())
            .buildCommunityCount(true)
            .buildHistogram(true);

        builder.withNodeCount(graph.nodeCount())
            .withConfig(config);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.buildResult());
        }

        PagedAtomicIntegerArray triangles = algorithm.getTriangles();

        return Stream.of(builder
            .withAverageClusteringCoefficient(algorithm.getAverageCoefficient())
            .withTriangleCount(algorithm.getTriangleCount())
            .withCommunityFunction(triangles::get)
            .withConfig(config)
            .withCreateMillis(computationResult.createMillis())
            .withComputeMillis(computationResult.computeMillis())
            .build()
        );
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

    public static class Result {
        public final long createMillis;
        public final long computeMillis;
        public final long postProcessingMillis;
        public final long nodeCount;
        public final long triangleCount;
        public final double averageClusteringCoefficient;
        public final long p1;
        public final long p5;
        public final long p10;
        public final long p25;
        public final long p50;
        public final long p75;
        public final long p90;
        public final long p95;
        public final long p99;
        public final long p100;

        public Result(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long nodeCount,
            long triangleCount,
            long p100,
            long p99,
            long p95,
            long p90,
            long p75,
            long p50,
            long p25,
            long p10,
            long p5,
            long p1,
            double averageClusteringCoefficient
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodeCount = nodeCount;
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            this.triangleCount = triangleCount;
            this.p100 = p100;
            this.p99 = p99;
            this.p95 = p95;
            this.p90 = p90;
            this.p75 = p75;
            this.p50 = p50;
            this.p25 = p25;
            this.p10 = p10;
            this.p5 = p5;
            this.p1 = p1;
        }
    }

    public class TriangleCountResultBuilder extends AbstractCommunityResultBuilder<Result> {

        private double averageClusteringCoefficient = .0;
        private long triangleCount = 0;

        public TriangleCountResultBuilder(ProcedureCallContext callContext, AllocationTracker tracker) {
            super(callContext, tracker);
        }


        public TriangleCountResultBuilder withAverageClusteringCoefficient(double averageClusteringCoefficient) {
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            return this;
        }

        public TriangleCountResultBuilder withTriangleCount(long triangleCount) {
            this.triangleCount = triangleCount;
            return this;
        }

        public TriangleCountResultBuilder buildHistogram(boolean buildHistogram) {
            this.buildHistogram = buildHistogram;
            return this;
        }

        public TriangleCountResultBuilder buildCommunityCount(boolean buildCommunityCount) {
            this.buildCommunityCount = buildCommunityCount;
            return this;
        }

        @Override
        protected Result buildResult() {
            return new Result(
                createMillis,
                computeMillis,
                postProcessingDuration,
                nodeCount,
                triangleCount,
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(100)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(99)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(95)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(90)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(75)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(50)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(25)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(10)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(5)).orElse(0L),
                maybeCommunityHistogram.map(h -> h.getValueAtPercentile(1)).orElse(0L),
                averageClusteringCoefficient
            );
        }
    }
}
