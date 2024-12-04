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
package org.neo4j.gds.applications.algorithms.miscellaneous;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.indexInverse.InverseRelationships;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfig;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfigTransformer;
import org.neo4j.gds.indexInverse.InverseRelationshipsProgressTaskCreator;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.undirected.ToUndirected;
import org.neo4j.gds.undirected.ToUndirectedConfig;
import org.neo4j.gds.walking.CollapsePath;
import org.neo4j.gds.walking.CollapsePathConfig;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MiscellaneousAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    public MiscellaneousAlgorithms(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    SingleTypeRelationships collapsePath(GraphStore graphStore, CollapsePathConfig configuration) {
        Collection<NodeLabel> nodeLabels = configuration.nodeLabelIdentifiers(graphStore);

        /*
         * here we build a graph-per-relationship type. you can think of them as layers.
         * the algorithm will take a step in a layer, then a next step in another layer.
         * that obviously stops of a node in a layer is not connected to anything.
         */
        List<Graph[]> pathTemplatesEncodedAsListsOfSingleRelationshipTypeGraphs = configuration.pathTemplates().stream()
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

        var algorithm = new CollapsePath(
            pathTemplatesEncodedAsListsOfSingleRelationshipTypeGraphs,
            configuration.allowSelfLoops(),
            RelationshipType.of(configuration.mutateRelationshipType()),
            configuration.concurrency(),
            DefaultPool.INSTANCE
        );

        return algorithm.compute();
    }

    Map<RelationshipType, SingleTypeRelationships> indexInverse(
        IdMap idMap,
        GraphStore graphStore,
        InverseRelationshipsConfig configuration
    ) {
        var parameters = InverseRelationshipsConfigTransformer.toParameters(configuration);
        var relationshipTypes = parameters.internalRelationshipTypes(graphStore);

        var task = InverseRelationshipsProgressTaskCreator.progressTask(idMap.nodeCount(), relationshipTypes);
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new InverseRelationships(
            graphStore,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    ScalePropertiesResult scaleProperties(Graph graph, ScalePropertiesBaseConfig configuration) {
        int totalPropertyDimension = configuration
            .nodeProperties()
            .stream()
            .map(graph::nodeProperties)
            .mapToInt(p -> p.dimension().orElseThrow(/* already validated in config */))
            .sum();
        var task = Tasks.task(
            AlgorithmLabel.ScaleProperties.asString(),
            Tasks.leaf("Prepare scalers", graph.nodeCount() * totalPropertyDimension),
            Tasks.leaf("Scale properties", graph.nodeCount() * totalPropertyDimension)
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new ScaleProperties(
            graph,
            configuration,
            progressTracker,
            DefaultPool.INSTANCE
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    public SingleTypeRelationships toUndirected(GraphStore graphStore, ToUndirectedConfig configuration) {
        var task = Tasks.task(
            AlgorithmLabel.ToUndirected.asString(),
            Tasks.leaf("Create Undirected Relationships", graphStore.nodeCount()),
            Tasks.leaf("Build undirected Adjacency list")
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new ToUndirected(
            graphStore,
            configuration,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }
}
