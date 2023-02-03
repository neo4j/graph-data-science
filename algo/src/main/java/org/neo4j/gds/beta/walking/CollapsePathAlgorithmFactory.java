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
package org.neo4j.gds.beta.walking;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CollapsePathAlgorithmFactory extends GraphStoreAlgorithmFactory<CollapsePath, CollapsePathConfig> {
    @Override
    public CollapsePath build(
        GraphStore graphStore,
        CollapsePathConfig config,
        ProgressTracker progressTracker
    ) {
        Collection<NodeLabel> nodeLabels = config.nodeLabelIdentifiers(graphStore);

        /*
         * here we build a graph-per-relationship type. you can think of them as layers.
         * the algorithm will take a step in a layer, then a next step in another layer.
         * that obviously stops of a node in a layer is not connected to anything.
         */
        List<Graph[]> pathTemplatesEncodedAsListsOfSingleRelationshipTypeGraphs = config.pathTemplates().stream()
            .map(
                path -> path.stream()
                    .map(
                        relationshipTypeAsString -> graphStore.getGraph(
                            nodeLabels,
                            Set.of(RelationshipType.of(relationshipTypeAsString)),
                            Optional.empty()
                        )
                    )
                    .toArray(Graph[]::new)
            )
            .collect(Collectors.toList());

        return new CollapsePath(
            pathTemplatesEncodedAsListsOfSingleRelationshipTypeGraphs,
            config.allowSelfLoops(),
            RelationshipType.of(config.mutateRelationshipType()),
            config.concurrency(),
            Pools.DEFAULT
        );
    }

    @Override
    public String taskName() {
        return "CollapsePath";
    }
}
