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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures;

import org.neo4j.gds.ml.TrainingConfig;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Responsible for extracting features on a specific graph.
 * Instances should not be reused between different graphs.
 */
public final class LinkFeatureExtractor {
    private final List<LinkFeatureAppender> linkFeatureAppenders;
    private final int featureDimension;
    private final List<Integer> featureDimensions;

    private LinkFeatureExtractor(
        List<LinkFeatureAppender> linkFeatureAppenders,
        int featureDimension,
        List<Integer> featureDimensions
    ) {
        this.linkFeatureAppenders = linkFeatureAppenders;
        this.featureDimension = featureDimension;
        this.featureDimensions = featureDimensions;
    }

    public static LinkFeatureExtractor of(Graph graph, List<LinkFeatureStep> linkFeatureSteps) {
        var linkFeatureProducers = linkFeatureSteps
            .stream()
            .map(step -> step.linkFeatureAppender(graph))
            .collect(Collectors.toList());

        var featureDimensions = linkFeatureSteps.stream().map(step -> step.outputFeatureDimension(graph)).collect(
            Collectors.toList());
        int featureDimension = featureDimensions.stream().mapToInt(Integer::intValue).sum();
        return new LinkFeatureExtractor(linkFeatureProducers, featureDimension, featureDimensions);
    }

    public static HugeObjectArray<double[]> extractFeatures(Graph graph, List<LinkFeatureStep> linkFeatureSteps, int concurrency) {
        var extractor = of(graph, linkFeatureSteps);

        var linkFeatures = HugeObjectArray.newArray(
            double[].class,
            graph.relationshipCount(),
            AllocationTracker.empty()
        );

        var partitions = PartitionUtils.degreePartition(
            graph,
            concurrency,
            Function.identity(),
            Optional.of(TrainingConfig.DEFAULT_BATCH_SIZE)
        );

        var linkFeatureWriters = new ArrayList<BatchLinkFeatureExtractor>();
        var relationshipOffset = 0L;
        for (DegreePartition partition : partitions) {
            linkFeatureWriters.add(new BatchLinkFeatureExtractor(
                extractor,
                partition,
                graph.concurrentCopy(),
                relationshipOffset,
                linkFeatures
            ));
            relationshipOffset += partition.totalDegree();
        }

        ParallelUtil.runWithConcurrency(concurrency, linkFeatureWriters, Pools.DEFAULT);

        return linkFeatures;
    }

    public double[] extractFeatures(long source, long target) {
        var featuresForLink = new double[featureDimension];
        int featureOffset = 0;
        for (int i = 0; i < linkFeatureAppenders.size(); i++) {
            var featureProducer = linkFeatureAppenders.get(i);
            featureProducer.appendFeatures(source, target, featuresForLink, featureOffset);
            featureOffset += featureDimensions.get(i);
        }
        return featuresForLink;
    }

    public int featureDimension() {
        return featureDimension;
    }
}
