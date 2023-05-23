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
package org.neo4j.gds.ml.pipeline.linkPipeline;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.gradientdescent.GradientDescentConfig;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

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
    private final boolean isSymmetric;

    private LinkFeatureExtractor(List<LinkFeatureAppender> linkFeatureAppenders) {
        this.linkFeatureAppenders = linkFeatureAppenders;
        this.featureDimension = linkFeatureAppenders.stream().mapToInt(LinkFeatureAppender::dimension).sum();
        this.isSymmetric = linkFeatureAppenders.stream().allMatch(LinkFeatureAppender::isSymmetric);
    }

    public static LinkFeatureExtractor of(Graph graph, List<LinkFeatureStep> linkFeatureSteps) {
        var linkFeatureProducers = linkFeatureSteps
            .stream()
            .map(step -> step.linkFeatureAppender(graph))
            .collect(Collectors.toList());

        return new LinkFeatureExtractor(linkFeatureProducers);
    }

    public static Features extractFeatures(
        Graph graph,
        List<LinkFeatureStep> linkFeatureSteps,
        int concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var extractor = of(graph, linkFeatureSteps);

        var linkFeatures = HugeObjectArray.newArray(
            double[].class,
            graph.relationshipCount()
        );

        var partitions = PartitionUtils.degreePartition(
            graph,
            concurrency,
            Function.identity(),
            Optional.of(GradientDescentConfig.DEFAULT_BATCH_SIZE)
        );

        var linkFeatureWriters = new ArrayList<BatchLinkFeatureExtractor>();
        var relationshipOffset = 0L;
        for (DegreePartition partition : partitions) {
            linkFeatureWriters.add(new BatchLinkFeatureExtractor(
                extractor,
                partition,
                graph.concurrentCopy(),
                relationshipOffset,
                linkFeatures,
                progressTracker
            ));
            relationshipOffset += partition.relationshipCount();
        }

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(linkFeatureWriters)
            .terminationFlag(terminationFlag)
            .run();

        return FeaturesFactory.wrap(linkFeatures);
    }

    public int featureDimension() {
        return featureDimension;
    }

    public double[] extractFeatures(long source, long target) {
        var featuresForLink = new double[featureDimension];
        int featureOffset = 0;
        for (LinkFeatureAppender featureProducer : linkFeatureAppenders) {
            featureProducer.appendFeatures(source, target, featuresForLink, featureOffset);
            featureOffset += featureProducer.dimension();
        }
        return featuresForLink;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }
}
