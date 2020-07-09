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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Map;
import java.util.Optional;

public class GraphInfoWithHistogram extends GraphInfo {

    private static final int PRECISION = 5;

    public final Map<String, Object> degreeDistribution;

    public GraphInfoWithHistogram(
        GraphInfo graphInfo,
        Map<String, Object> degreeDistribution
    ) {
        super(
            graphInfo.graphName,
            graphInfo.memoryUsage,
            graphInfo.sizeInBytes,
            graphInfo.nodeProjection,
            graphInfo.relationshipProjection,
            graphInfo.nodeQuery,
            graphInfo.relationshipQuery,
            graphInfo.nodeCount,
            graphInfo.relationshipCount,
            graphInfo.creationTime,
            graphInfo.modificationTime,
            graphInfo.schema
        );
        this.degreeDistribution = degreeDistribution;
    }

    static GraphInfoWithHistogram of(GraphCreateConfig graphCreateConfig, GraphStore graphStore, NamedDatabaseId namedDatabaseId) {
        var graphInfo = GraphInfo.of(graphCreateConfig, graphStore);

        Optional<Map<String, Object>> maybeDegreeDistribution = GraphStoreCatalog.getDegreeDistribution(
            graphCreateConfig.username(),
            namedDatabaseId,
            graphCreateConfig.graphName()
        );

        var degreeDistribution = maybeDegreeDistribution.orElseGet(() -> {
            var newHistogram = computeHistogram(graphStore);
            GraphStoreCatalog.setDegreeDistribution(
                graphCreateConfig.username(),
                namedDatabaseId,
                graphCreateConfig.graphName(),
                newHistogram
            );
            return newHistogram;
        });

        return new GraphInfoWithHistogram(graphInfo, degreeDistribution);

    }

    private static Map<String, Object> computeHistogram(GraphStore graphStore) {
        Graph graph = graphStore.getUnion();
        int batchSize = Math.toIntExact(ParallelUtil.adjustedBatchSize(
            graph.nodeCount(),
            ConcurrencyConfig.DEFAULT_CONCURRENCY,
            ParallelUtil.DEFAULT_BATCH_SIZE
        ));
        // needs to be at least 2 due to some requirement from the AtomicHistogram, see their JavaDoc
        long maximumDegree = Math.max(2, graph.relationshipCount());
        AtomicHistogram histogram = new AtomicHistogram(maximumDegree, PRECISION);

        ParallelUtil.readParallel(
            ConcurrencyConfig.DEFAULT_CONCURRENCY,
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
