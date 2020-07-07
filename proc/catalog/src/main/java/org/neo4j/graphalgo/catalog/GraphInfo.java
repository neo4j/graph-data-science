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

import static java.util.Collections.emptyMap;

public class GraphInfo extends GraphInfoWithoutDegreeDistribution {
    private static final int PRECISION = 5;
    private static NamedDatabaseId namedDatabaseId;

    public final String userName;
    public final Map<String, Object> degreeDistribution;

    GraphInfo(GraphCreateConfig config, NamedDatabaseId namedDatabaseId, GraphStore graphStore, boolean computeHistogram) {
        super(config, graphStore);
        GraphInfo.namedDatabaseId = namedDatabaseId;
        this.userName = config.username();
        this.degreeDistribution = computeHistogram ? computeHistogram(graphStore) : emptyMap();
    }

    private Map<String, Object> computeHistogram(GraphStore graphStore) {
        Optional<Map<String, Object>> maybeDegreeDistribution = lookupDegreeDistribution();

        if (maybeDegreeDistribution.isPresent()) {
            return maybeDegreeDistribution.get();
        }

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
        Map<String, Object> degreeDistribution = MapUtil.map(
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

        GraphStoreCatalog.setDegreeDistribution(userName, namedDatabaseId, graphName, degreeDistribution);

        return degreeDistribution;
    }

    private Optional<Map<String, Object>> lookupDegreeDistribution() {
        return GraphStoreCatalog.getDegreeDistribution(userName, namedDatabaseId, graphName);
    }

}
