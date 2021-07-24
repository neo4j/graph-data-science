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

public class LinkFeatureExtractor {
    private final List<LinkFeatureAppender> linkFeatureAppenders;
    private final int totalFeatureSize;
    private final List<Integer> featureSizes;

    LinkFeatureExtractor(
        List<LinkFeatureAppender> linkFeatureAppenders,
        int totalFeatureSize,
        List<Integer> featureSizes
    ) {
        this.linkFeatureAppenders = linkFeatureAppenders;
        this.totalFeatureSize = totalFeatureSize;
        this.featureSizes = featureSizes;
    }

    public static LinkFeatureExtractor of(Graph graph, List<LinkFeatureStep> linkFeatureSteps) {
        var linkFeatureProducers = linkFeatureSteps
            .stream()
            .map(step -> step.linkFeatureAppender(graph))
            .collect(Collectors.toList());

        var featureSize = linkFeatureSteps.stream().map(step -> step.outputFeatureSize(graph)).collect(
            Collectors.toList());
        int totalFeatureSize = featureSize.stream().mapToInt(Integer::intValue).sum();
        return new LinkFeatureExtractor(linkFeatureProducers, totalFeatureSize, featureSize);
    }

    public static HugeObjectArray<double[]> extractFeatures(Graph graph, List<LinkFeatureStep> linkFeatureSteps) {
        var multiLinkFeatureProducer = of(graph, linkFeatureSteps);

        var linkFeatures = HugeObjectArray.newArray(
            double[].class,
            graph.relationshipCount(),
            AllocationTracker.empty()
        );

        var relationshipOffset = new MutableLong();
        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                linkFeatures.set(relationshipOffset.getAndIncrement(), multiLinkFeatureProducer.extractFeatures(source, target));
                return true;
            });
            return true;
        });
        return linkFeatures;
    }

    private double[] extractFeatures(long source, long target) {
        var featuresForLink = new double[totalFeatureSize];
        int featureOffset = 0;
        for (int i = 0; i < linkFeatureAppenders.size(); i++) {
            var featureProducer = linkFeatureAppenders.get(i);
            featureProducer.addFeatures(source, target, featuresForLink, featureOffset);
            featureOffset += featureSizes.get(i);
        }
        return featuresForLink;
    }
}
