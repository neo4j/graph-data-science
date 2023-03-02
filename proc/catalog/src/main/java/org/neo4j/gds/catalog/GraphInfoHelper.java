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
package org.neo4j.gds.catalog;

import org.HdrHistogram.AtomicHistogram;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.Map;

public final class GraphInfoHelper {

    /**
     * Needs to be at least 2 due to some requirement from the AtomicHistogram.
     *
     * @see org.HdrHistogram.Histogram
     */
    private static final int PRECISION = 5;

    private GraphInfoHelper() {}

    public static Map<String, Object> degreeDistribution(Graph graph, TerminationFlag terminationFlag) {
        long maximumDegree = Math.max(2, graph.relationshipCount());
        AtomicHistogram histogram = new AtomicHistogram(maximumDegree, PRECISION);

        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            ConcurrencyConfig.DEFAULT_CONCURRENCY,
            terminationFlag,
            nodeId -> histogram.recordValue(graph.degree(nodeId))
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

    public static double density(long nodeCount, long relationshipCount) {
        return (nodeCount > 0L) ? (double) relationshipCount / (nodeCount * (nodeCount - 1)) : 0;
    }

    public static double density(Graph graph) {
        return density(graph.nodeCount(), graph.relationshipCount());
    }

}
