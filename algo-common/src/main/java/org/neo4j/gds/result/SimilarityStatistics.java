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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.ProcedureConstants;
import org.neo4j.gds.core.utils.ProgressTimer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class SimilarityStatistics {


    public static SimilarityStats similarityStats(
        Supplier<Graph> maybeSimilarityGraph,
        boolean shouldComputeDistribution
    ) {
        return  similarityStats(
            maybeSimilarityGraph,
            shouldComputeDistribution,
            () ->new DoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT)
        );
    }


    public static SimilarityStats similarityStats(
        Supplier<Graph> maybeSimilarityGraph,
        boolean shouldComputeDistribution,
        Supplier<DoubleHistogram> histogramSupplier
        ) {

        if (!shouldComputeDistribution) {
            return new SimilarityStats(Optional.empty(), 0,true);
        }
        Histogram histogram;
        var computeMilliseconds = new AtomicLong(0);
        try(var ignored = ProgressTimer.start(computeMilliseconds::set)) {
             histogram = computeHistogram(maybeSimilarityGraph.get(),histogramSupplier);
        }
        return new SimilarityStats( histogram.histogram(), computeMilliseconds.get(), histogram.success());

    }

    public static Map<String, Object> similaritySummary(Optional<DoubleHistogram> histogram, boolean success) {
       if (!success){
           return HistogramUtils.failure();
       }
        return histogram
            .map(HistogramUtils::similaritySummary)
            .orElseGet(Collections::emptyMap);
    }

    public static Histogram computeHistogram(Graph similarityGraph, Supplier<DoubleHistogram> histogramSupplier) {
        
        DoubleHistogram histogram = histogramSupplier.get();
        try {
            similarityGraph.forEachNode(nodeId -> {
                similarityGraph.forEachRelationship(nodeId, Double.NaN, (node1, node2, property) -> {

                    histogram.recordValue(property);
                    return true;
                });
                return true;
            });
        } catch (ArrayIndexOutOfBoundsException e) {

            if (e.getMessage().contains("is out of bounds for histogram, current covered range")) {
                return  new Histogram(Optional.of(histogram),false);
            }else{
                throw  e;
            }
        }

        return new Histogram(Optional.of(histogram),true);

    }

    private SimilarityStatistics() {}
    public record  Histogram(Optional<DoubleHistogram> histogram,  boolean success) {}

    public record  SimilarityStats(Optional<DoubleHistogram> histogram, long computeMilliseconds, boolean success) {}
}
