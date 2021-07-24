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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Responsible for extracting features on a specific graph.
 * Instances should not be reused between different graphs.
 */
public class LinkFeatureExtractor {
    private final List<LinkFeatureAppender> linkFeatureAppenders;
    private final int featureDimension;
    private final List<Integer> featureDimensions;

    LinkFeatureExtractor(
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

    public static HugeObjectArray<double[]> extractFeatures(Graph graph, List<LinkFeatureStep> linkFeatureSteps) {
        var extractor = of(graph, linkFeatureSteps);

        var linkFeatures = HugeObjectArray.newArray(
            double[].class,
            graph.relationshipCount(),
            AllocationTracker.empty()
        );

        var relationshipOffset = new MutableLong();
        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                linkFeatures.set(relationshipOffset.getAndIncrement(), extractor.extractFeatures(source, target));
                return true;
            });
            return true;
        });
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

    public int featureDimension () {
        return featureDimension;
    }
}
