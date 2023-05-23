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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.gradientdescent.GradientDescentConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.neo4j.gds.ml.negativeSampling.NegativeSampler.NEGATIVE;
import static org.neo4j.gds.ml.splitting.EdgeSplitter.POSITIVE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class LinkFeaturesAndLabelsExtractor {

    private LinkFeaturesAndLabelsExtractor() {}

    static MemoryEstimation estimate(
        MemoryRange fudgedLinkFeatureDim,
        ToLongFunction<Map<RelationshipType, Long>> relSetSizeExtractor,
        String setDesc
    ) {
        return MemoryEstimations
            .builder()
            .rangePerGraphDimension(setDesc + " relationship features", (graphDim, threads) -> fudgedLinkFeatureDim
                .apply(MemoryUsage::sizeOfDoubleArray)
                .times(relSetSizeExtractor.applyAsLong(graphDim.relationshipCounts()))
                .add(MemoryUsage.sizeOfInstance(HugeObjectArray.class)))
            .perGraphDimension(
                setDesc + " relationship targets",
                (graphDim, threads) -> MemoryRange.of(
                    HugeIntArray.memoryEstimation(relSetSizeExtractor.applyAsLong(graphDim.relationshipCounts()))
                )
            ).build();
    }

    static FeaturesAndLabels extractFeaturesAndLabels(
        Graph graph,
        List<LinkFeatureStep> featureSteps,
        int concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        progressTracker.setSteps(graph.relationshipCount() * 2);
        var features = LinkFeatureExtractor.extractFeatures(
            graph,
            featureSteps,
            concurrency,
            progressTracker,
            terminationFlag
        );

        var labels = extractLabels(graph, features.size(), concurrency, progressTracker, terminationFlag);

        return ImmutableFeaturesAndLabels.of(features, labels);
    }

    private static HugeIntArray extractLabels(
        Graph graph,
        long numberOfTargets,
        int concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var globalLabels = HugeIntArray.newArray(numberOfTargets);
        var partitions = PartitionUtils.degreePartition(
            graph,
            concurrency,
            Function.identity(),
            Optional.of(GradientDescentConfig.DEFAULT_BATCH_SIZE)
        );
        var tasks = new ArrayList<Runnable>();
        final var relationshipOffset = new MutableLong();
        for (DegreePartition partition : partitions) {
            var startRelationshipOffset = relationshipOffset.getValue();
            tasks.add(() -> {
                var currentRelationshipOffset = new MutableLong(startRelationshipOffset);
                var localGraph = graph.concurrentCopy();
                partition.consume(nodeId -> localGraph.forEachRelationship(nodeId, -10, (src, trg, weight) -> {
                    if (weight == NEGATIVE || weight == POSITIVE) {
                        globalLabels.set(currentRelationshipOffset.getAndIncrement(), (int) weight);
                    } else {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Label should be either `1` or `0`. But got %f for relationship (%d, %d)",
                            weight,
                            src,
                            trg
                        ));
                    }
                    return true;
                }));
                    progressTracker.logSteps(partition.relationshipCount());
                }
            );
            relationshipOffset.add(partition.relationshipCount());
        }

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();

        return globalLabels;
    }
}
