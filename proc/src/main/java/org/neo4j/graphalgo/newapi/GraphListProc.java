/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.newapi;

import org.HdrHistogram.AtomicHistogram;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.neo4j.graphalgo.newapi.GraphCatalogProcs.HISTOGRAM_FIELD_NAME;

public class GraphListProc extends BaseProc {

    @Procedure(name = "algo.beta.graph.list", mode = Mode.READ)
    public Stream<GraphInfo> list(@Name(value = "graphName", defaultValue = "null") Object graphName) {
        if (Objects.nonNull(graphName) && !(graphName instanceof String)) {
            throw new IllegalArgumentException("`graphName` parameter must be a STRING");
        }

        boolean computeHistogram = callContext.outputFields().anyMatch(HISTOGRAM_FIELD_NAME::equals);
        Stream<Map.Entry<GraphCreateConfig, Graph>> graphEntries;

        graphEntries = GraphCatalog.getLoadedGraphs(getUsername()).entrySet().stream();
        if (!isEmpty((String)graphName)) {
            // we should only list the provided graph
            graphEntries = graphEntries.filter(e -> e.getKey().graphName().equals(graphName));
        }

        return graphEntries.map(e -> new GraphInfo(e.getKey(), e.getValue(), computeHistogram));
    }

    public static class GraphInfo {

        public final String graphName;
        public final Map<String, Object> nodeProjection, relationshipProjection;
        public final long nodes, relationships;
        public final Map<String, Object> histogram;

        GraphInfo(GraphCreateConfig config, Graph graph, boolean computeHistogram) {
            this.graphName = config.graphName();
            nodeProjection = config.nodeProjection().toObject();
            relationshipProjection = config.relationshipProjection().toObject();
            nodes = graph.nodeCount();
            relationships = graph.relationshipCount();
            histogram = computeHistogram ? computeHistogram(graph) : emptyMap();
        }

        private static final int PRECISION = 5;

        private Map<String, Object> computeHistogram(Graph graph) {
            int batchSize = Math.toIntExact(ParallelUtil.adjustedBatchSize(
                graph.nodeCount(),
                Pools.DEFAULT_CONCURRENCY,
                ParallelUtil.DEFAULT_BATCH_SIZE
            ));
            // needs to be at least 2 due to some requirement from the AtomicHistogram, see their JavaDoc
            long maximumDegree = Math.max(2, graph.relationshipCount());
            AtomicHistogram histogram = new AtomicHistogram(maximumDegree, PRECISION);

            ParallelUtil.readParallel(
                Pools.DEFAULT_CONCURRENCY,
                batchSize,
                graph,
                Pools.DEFAULT,
                (nodeOffset, nodeIds) -> () -> {
                    PrimitiveLongIterator iterator = nodeIds.iterator();
                    while (iterator.hasNext()) {
                        long nodeId = iterator.next();
                        int degree = graph.degree(nodeId, Direction.OUTGOING);
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
}
