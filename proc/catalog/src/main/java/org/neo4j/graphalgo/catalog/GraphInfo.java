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
package org.neo4j.graphalgo.catalog;

import org.HdrHistogram.AtomicHistogram;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.time.ZonedDateTime;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class GraphInfo {
    private static final int PRECISION = 5;

    public final String graphName;
    public final String memoryUsage;
    public final long sizeInBytes;
    public final Map<String, Object> nodeProjection;
    public final Map<String, Object> relationshipProjection;
    public final String nodeQuery;
    public final String relationshipQuery;
    public final long nodeCount;
    public final long relationshipCount;
    public final Map<String, Object> degreeDistribution;
    public final ZonedDateTime creationTime;
    public final ZonedDateTime modificationTime;

    GraphInfo(GraphCreateConfig config, GraphStore graphStore, boolean computeHistogram) {
        this.graphName = config.graphName();
        this.creationTime = config.creationTime();

        if (config instanceof GraphCreateFromCypherConfig) {
            GraphCreateFromCypherConfig cypherConfig = (GraphCreateFromCypherConfig) config;
            this.nodeQuery = cypherConfig.nodeQuery();
            this.relationshipQuery = cypherConfig.relationshipQuery();
            this.nodeProjection = null;
            this.relationshipProjection = null;
        } else {
            this.nodeProjection = config.nodeProjections().toObject();
            this.relationshipProjection = config.relationshipProjections().toObject();
            this.nodeQuery = null;
            this.relationshipQuery = null;
        }

        this.modificationTime = graphStore.modificationTime();
        this.nodeCount = graphStore.nodeCount();
        this.relationshipCount = graphStore.relationshipCount();
        this.degreeDistribution = computeHistogram ? computeHistogram(graphStore.getUnion()) : emptyMap();
        this.sizeInBytes = MemoryUsage.sizeOf(graphStore);
        this.memoryUsage = MemoryUsage.humanReadable(this.sizeInBytes);
    }

    private Map<String, Object> computeHistogram(Graph graph) {
        int batchSize = Math.toIntExact(ParallelUtil.adjustedBatchSize(
            graph.nodeCount(),
            AlgoBaseConfig.DEFAULT_CONCURRENCY,
            ParallelUtil.DEFAULT_BATCH_SIZE
        ));
        // needs to be at least 2 due to some requirement from the AtomicHistogram, see their JavaDoc
        long maximumDegree = Math.max(2, graph.relationshipCount());
        AtomicHistogram histogram = new AtomicHistogram(maximumDegree, PRECISION);

        ParallelUtil.readParallel(
            AlgoBaseConfig.DEFAULT_CONCURRENCY,
            batchSize,
            graph,
            Pools.DEFAULT,
            (nodeOffset, nodeIds) -> () -> {
                PrimitiveLongIterator iterator = nodeIds.iterator();
                while (iterator.hasNext()) {
                    long nodeId = iterator.next();
                    int degree = graph.degree(nodeId);
                    histogram.recordValue(degree);
                }
            }
        );
        return MapUtil.map(
            "min", histogram.getMinValue(),
            "mean", histogram.getMean(),
            "max", histogram.getMaxValue(),
            "p50", histogram.getValueAtPercentile(50),
            "p75", histogram.getValueAtPercentile(75),
            "p90", histogram.getValueAtPercentile(90),
            "p95", histogram.getValueAtPercentile(95),
            "p99", histogram.getValueAtPercentile(99),
            "p999", histogram.getValueAtPercentile(99.9)
        );
    }
}
