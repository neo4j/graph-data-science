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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Pipeline {
    private List<LinkFeatureStep> linkFeatureSteps;

    public Pipeline() {
        this.linkFeatureSteps = new ArrayList<>();
    }

    public void addLinkFeature(String name, Map<String, Object> config) {
        this.linkFeatureSteps.add(LinkFeatureStepFactory.create(name, config));
    }

    public HugeObjectArray<double[]> computeLinkFeatures(Graph graph) {
        validate(graph);

        var linkFeatures = HugeObjectArray.newArray(
            double[].class,
            graph.relationshipCount(),
            AllocationTracker.empty()
        );

        List<Integer> featureSize = linkFeatureSteps.stream().map(step -> step.outputFeatureSize(graph)).collect(Collectors.toList());

        int totalFeatureSize = featureSize.stream().mapToInt(Integer::intValue).sum();
        linkFeatures.setAll(i -> new double[totalFeatureSize]);

        int featureOffset = 0;

        for (int i = 0; i < linkFeatureSteps.size(); i++) {
            LinkFeatureStep step = linkFeatureSteps.get(i);
            step.addFeatures(graph, linkFeatures, featureOffset);
            featureOffset += featureSize.get(i);
        }

        return linkFeatures;
    }

    private void validate(Graph graph) {
        Set<String> graphProperties = graph.availableNodeProperties();

        var invalidProperties = linkFeatureSteps
            .stream()
            .flatMap(step -> step.inputNodeProperties().stream())
            .filter(property -> !graphProperties.contains(property))
            .collect(Collectors.toList());

        if (!invalidProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale("Node properties %s defined in the LinkFeatureSteps do not exist in the graph or part of the pipeline", invalidProperties));
        }
    }
}
