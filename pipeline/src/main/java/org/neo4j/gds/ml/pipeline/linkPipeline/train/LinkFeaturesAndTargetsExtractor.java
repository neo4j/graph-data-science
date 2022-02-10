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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;

import java.util.List;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class LinkFeaturesAndTargetsExtractor {

    private LinkFeaturesAndTargetsExtractor() {}

    static MemoryEstimation estimate(
        MemoryRange fudgedLinkFeatureDim,
        LongUnaryOperator relSetSizeExtractor,
        String setDesc
    ) {
        return MemoryEstimations
            .builder()
            .rangePerGraphDimension(setDesc + " relationship features", (graphDim, threads) -> fudgedLinkFeatureDim
                .apply(MemoryUsage::sizeOfDoubleArray)
                .times(relSetSizeExtractor.applyAsLong(graphDim.relCountUpperBound()))
                .add(MemoryUsage.sizeOfInstance(HugeObjectArray.class)))
            .perGraphDimension(
                setDesc + "relationship targets",
                (graphDim, threads) -> MemoryRange.of(
                    HugeDoubleArray.memoryEstimation(relSetSizeExtractor.applyAsLong(graphDim.relCountUpperBound()))
                )
            ).build();
    }

    static FeaturesAndTargets extractFeaturesAndTargets(
        Graph graph,
        List<LinkFeatureStep> featureSteps,
        int concurrency,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        progressTracker.setVolume(graph.relationshipCount() * 2);
        var features = LinkFeatureExtractor.extractFeatures(graph, featureSteps, concurrency, progressTracker);

        var targets = extractTargets(graph, features.size(), progressTracker, allocationTracker);

        return ImmutableFeaturesAndTargets.of(features, targets);
    }

    private static HugeDoubleArray extractTargets(
        Graph graph, long numberOfTargets, ProgressTracker progressTracker, AllocationTracker allocationTracker
    ) {
        var globalTargets = HugeDoubleArray.newArray(numberOfTargets, allocationTracker);
        var relationshipIdx = new MutableLong();
        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, -10, (src, trg, weight) -> {
                if (weight == 0.0D || weight == 1.0D) {
                    globalTargets.set(relationshipIdx.getAndIncrement(), weight);
                } else {
                    throw new IllegalArgumentException(formatWithLocale("Target should be either `1` or `0`. But got %f for relationship (%d, %d)",
                        weight,
                        src,
                        trg
                    ));
                }
                return true;
            });
            progressTracker.logProgress(graph.degree(nodeId));
            return true;
        });
        return globalTargets;
    }
}
