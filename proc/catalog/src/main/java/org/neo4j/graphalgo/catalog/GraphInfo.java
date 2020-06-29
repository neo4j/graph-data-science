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
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

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
    public final Map<String, Object> schema;

    GraphInfo(
        GraphCreateConfig config,
        GraphStore graphStore,
        boolean computeHistogram,
        Optional<Map<String, Object>> maybeDegreeDistribution
    ) {
        this.graphName = config.graphName();
        this.creationTime = config.creationTime();

        if (config instanceof GraphCreateFromCypherConfig) {
            GraphCreateFromCypherConfig cypherConfig = (GraphCreateFromCypherConfig) config;
            this.nodeQuery = cypherConfig.nodeQuery();
            this.relationshipQuery = cypherConfig.relationshipQuery();
            this.nodeProjection = null;
            this.relationshipProjection = null;
        } else if (config instanceof RandomGraphGeneratorConfig) {
            RandomGraphGeneratorConfig randomGraphConfig = (RandomGraphGeneratorConfig) config;
            this.nodeProjection = randomGraphConfig.nodeProjections().toObject();
            this.relationshipProjection = randomGraphConfig.relationshipProjections().toObject();
            this.nodeQuery = null;
            this.relationshipQuery = null;
        } else {
            GraphCreateFromStoreConfig fromStoreConfig = (GraphCreateFromStoreConfig) config;
            this.nodeProjection = fromStoreConfig.nodeProjections().toObject();
            this.relationshipProjection = fromStoreConfig.relationshipProjections().toObject();
            this.nodeQuery = null;
            this.relationshipQuery = null;
        }

        this.modificationTime = graphStore.modificationTime();
        this.nodeCount = graphStore.nodeCount();
        this.relationshipCount = graphStore.relationshipCount();
        this.schema = graphStore.schema().toMap();
        this.sizeInBytes = MemoryUsage.sizeOf(graphStore);
        this.memoryUsage = MemoryUsage.humanReadable(this.sizeInBytes);
        this.degreeDistribution = computeHistogram ? computeHistogram(graphStore, maybeDegreeDistribution) : emptyMap();
        if (GraphStoreCatalog.exists(config.username(), config.graphName())) {
            GraphStoreCatalog
                .getUserCatalog(config.username())
                .setDegreeDistribution(config.graphName(), this.degreeDistribution);
        }
    }

    private Map<String, Object> computeHistogram(GraphStore graphStore, Optional<Map<String, Object>> maybeDegreeDistribution) {
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
