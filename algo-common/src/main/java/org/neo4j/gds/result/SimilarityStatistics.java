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
package org.neo4j.gds.result;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.ProgressTimer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

public final class SimilarityStatistics {




    @ValueClass
    @SuppressWarnings("immutables:incompat")
    public interface SimilarityStats {

        Optional<DoubleHistogram> histogram();
        long computeMilliseconds();
    }

    public static SimilarityStats similarityStats(
        Supplier<Graph> maybeSimilarityGraph,
        boolean shouldComputeDistribution
        ) {
        if (!shouldComputeDistribution) {
            return ImmutableSimilarityStats.of(Optional.empty(), 0);

        }
        Optional<DoubleHistogram> maybeHistogram;
        var computeMilliseconds = new AtomicLong(0);
        try(var ignored = ProgressTimer.start(computeMilliseconds::set)) {
            maybeHistogram = computeHistogram(maybeSimilarityGraph.get());
        }

        return ImmutableSimilarityStats.of( maybeHistogram, computeMilliseconds.get());
    }

    public static Map<String, Object> similaritySummary(Optional<DoubleHistogram> histogram) {
        return histogram
            .map(HistogramUtils::similaritySummary)
            .orElseGet(Collections::emptyMap);
    }

    public static Optional<DoubleHistogram> computeHistogram(Graph similarityGraph) {
        
        DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
        similarityGraph.forEachNode(nodeId -> {
            similarityGraph.forEachRelationship(nodeId, Double.NaN, (node1, node2, property) -> {
                histogram.recordValue(property);
                return true;
            });
            return true;
        });
        return Optional.of(histogram);

    }


}
