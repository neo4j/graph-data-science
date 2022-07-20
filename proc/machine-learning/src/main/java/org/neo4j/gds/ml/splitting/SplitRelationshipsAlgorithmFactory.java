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
package org.neo4j.gds.ml.splitting;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Collection;
import java.util.Optional;

public class SplitRelationshipsAlgorithmFactory extends GraphStoreAlgorithmFactory<SplitRelationships, SplitRelationshipsMutateConfig> {

    @Override
    public String taskName() {
        return "SplitRelationships";
    }

    @Override
    public SplitRelationships build(
        GraphStore graphStore,
        SplitRelationshipsMutateConfig configuration,
        ProgressTracker progressTracker
    ) {
        Optional<String> weightProperty = configuration != null
            ? Optional.ofNullable(configuration.relationshipWeightProperty())
            : Optional.empty();

        Collection<NodeLabel> nodeLabels = configuration.nodeLabelIdentifiers(graphStore);
        Collection<RelationshipType> relationshipTypes = configuration.internalRelationshipTypes(graphStore);

        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, weightProperty);
        var masterGraph = graph;
        if (!configuration.nonNegativeRelationshipTypes().isEmpty()) {
            masterGraph = graphStore.getGraph(
                configuration.nodeLabelIdentifiers(graphStore),
                configuration.superRelationshipTypes(),
                Optional.empty()
            );
        }
        return new SplitRelationships(graph, masterGraph, configuration, configuration.internalSourceLabels(graphStore), configuration.internalTargetLabels(graphStore));
    }

    @Override
    public MemoryEstimation memoryEstimation(SplitRelationshipsMutateConfig configuration) {
        return SplitRelationships.estimate(configuration);
    }
}
