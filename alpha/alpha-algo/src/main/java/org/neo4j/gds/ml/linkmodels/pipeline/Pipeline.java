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

public class Pipeline {
    private List<LinkFeatureStep> linkFeatureSteps;

    public Pipeline() {
        this.linkFeatureSteps = new ArrayList<>();
    }

    public void addLinkFeature(String name, Map<String, Object> config) {
        this.linkFeatureSteps.add(LinkFeatureStepFactory.create(name, config));
    }

    public HugeObjectArray<double[]> computeLinkFeatures(Graph graph) {
        var linkFeatures = HugeObjectArray.newArray(
            double[].class,
            graph.relationshipCount(),
            AllocationTracker.empty()
        );

        // FIXME: compute it based on samples for now? (Ideally its known on the property)
        int featureSize = 2;

        linkFeatures.setAll(i -> new double[featureSize]);

        int featureOffset = 0;
        for (LinkFeatureStep step : linkFeatureSteps) {
            step.addFeatures(graph, linkFeatures, featureOffset);
            // FIXME get the actual dimension (could sample the properties f.i.)
            featureOffset += 1;
        }

        return linkFeatures;
    }
}
