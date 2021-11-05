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
package org.neo4j.gds;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConfigurableSeedConfig;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.config.SourceNodeConfig;
import org.neo4j.gds.config.SourceNodesConfig;
import org.neo4j.gds.config.TargetNodeConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GraphStoreValidation {

    public static void validate(GraphStore graphStore, AlgoBaseConfig config) {
        Collection<NodeLabel> filterLabels = config.nodeLabelIdentifiers(graphStore);
        if (config instanceof SeedConfig) {
            validateSeedProperty(graphStore, (SeedConfig) config, filterLabels);
        }
        if (config instanceof ConfigurableSeedConfig) {
            validateConfigurableSeedProperty(graphStore, (ConfigurableSeedConfig) config, filterLabels);
        }
        if (config instanceof FeaturePropertiesConfig) {
            validateFeaturesProperties(graphStore, (FeaturePropertiesConfig) config, filterLabels);
        }
        if (config instanceof MutatePropertyConfig) {
            validateMutateProperty(graphStore, filterLabels, (MutatePropertyConfig) config);
        }
        if (config instanceof MutateRelationshipConfig) {
            validateMutateRelationships(graphStore, (MutateRelationshipConfig) config);
        }
        if (config instanceof SourceNodesConfig) {
            validateSourceNodes(graphStore, (SourceNodesConfig) config, filterLabels);

        }
        if (config instanceof SourceNodeConfig) {
            validateSourceNode(graphStore, (SourceNodeConfig) config, filterLabels);
        }
        if (config instanceof TargetNodeConfig) {
            validateTargetNode(graphStore, (TargetNodeConfig) config, filterLabels);
        }
    }

    private static void validateSeedProperty(GraphStore graphStore, SeedConfig config, Collection<NodeLabel> filterLabels) {
        String seedProperty = config.seedProperty();
        if (seedProperty != null && !graphStore.hasNodeProperty(filterLabels, seedProperty)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Seed property `%s` not found in graph with node properties: %s",
                seedProperty,
                graphStore.nodePropertyKeys().values()
            ));
        }
    }

    private static void validateConfigurableSeedProperty(GraphStore graphStore, ConfigurableSeedConfig config, Collection<NodeLabel> filterLabels) {
        String seedProperty = config.seedProperty();
        if (seedProperty != null && !graphStore.hasNodeProperty(filterLabels, seedProperty)) {
            throw new IllegalArgumentException(formatWithLocale(
                "`%s`: `%s` not found in graph with node properties: %s",
                config.propertyNameOverride(),
                seedProperty,
                graphStore.nodePropertyKeys().values()
            ));
        }
    }

    private static void validateMutateProperty(GraphStore graphStore, Collection<NodeLabel> filterLabels, MutatePropertyConfig mutateConfig) {
        String mutateProperty = mutateConfig.mutateProperty();

        if (mutateProperty != null && graphStore.hasNodeProperty(filterLabels, mutateProperty)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Node property `%s` already exists in the in-memory graph.",
                mutateProperty
            ));
        }
    }

    private static void validateMutateRelationships(GraphStore graphStore, MutateRelationshipConfig config) {
        String mutateRelationshipType = config.mutateRelationshipType();
        if (mutateRelationshipType != null && graphStore.hasRelationshipType(RelationshipType.of(mutateRelationshipType))) {
            throw new IllegalArgumentException(formatWithLocale(
                "Relationship type `%s` already exists in the in-memory graph.",
                mutateRelationshipType
            ));
        }
    }

    private static void validateFeaturesProperties(GraphStore graphStore, FeaturePropertiesConfig config, Collection<NodeLabel> filterLabels) {
        List<String> weightProperties = config.featureProperties();
        List<String> missingProperties;
        if (config.propertiesMustExistForEachNodeLabel()) {
            missingProperties = weightProperties
                .stream()
                .filter(weightProperty -> !graphStore.hasNodeProperty(filterLabels, weightProperty))
                .collect(Collectors.toList());
        } else {
            var availableProperties = filterLabels
                .stream().flatMap(label -> graphStore.nodePropertyKeys(label).stream()).collect(Collectors.toSet());
            missingProperties = new ArrayList<>(weightProperties);
            missingProperties.removeAll(availableProperties);
        }
        if (!missingProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The feature properties %s are not present for all requested labels. Requested labels: %s. Properties available on all requested labels: %s",
                StringJoining.join(missingProperties),
                StringJoining.join(filterLabels.stream().map(NodeLabel::name)),
                StringJoining.join(graphStore.nodePropertyKeys(filterLabels))
            ));
        }
    }

    private static void validateSourceNode(
        GraphStore graphStore,
        SourceNodeConfig config,
        Collection<NodeLabel> filterLabels
    ) {
        var sourceNodeId = config.sourceNode();

        if (labelFilteredGraphContainsNode(filterLabels, graphStore.nodes(), sourceNodeId)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Source node does not exist in the in-memory graph: `%d`",
                sourceNodeId
            ));
        }
    }

    private static void validateSourceNodes(GraphStore graphStore, SourceNodesConfig config, Collection<NodeLabel> filteredNodeLabels) {
        if (!config.sourceNodes().isEmpty()) {

            var missingNodes = config
                .sourceNodes()
                .stream()
                .filter(sourceNode -> labelFilteredGraphContainsNode(filteredNodeLabels, graphStore.nodes(), sourceNode))
                .map(Object::toString)
                .collect(Collectors.toList());

            if (!missingNodes.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Source nodes do not exist in the in-memory graph or do not have the specified node labels: %s",
                    StringJoining.join(missingNodes)
                ));
            }
        }
    }

    private static void validateTargetNode(
        GraphStore graphStore,
        TargetNodeConfig config,
        Collection<NodeLabel> filterLabels
    ) {
        var targetNodeId = config.targetNode();

        if (labelFilteredGraphContainsNode(filterLabels, graphStore.nodes(), targetNodeId)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Target node does not exist in the in-memory graph: `%d`",
                targetNodeId
            ));
        }
    }

    private static boolean labelFilteredGraphContainsNode(
        Collection<NodeLabel> filteredNodeLabels,
        NodeMapping nodeMapping,
        long neoNodeId
    ) {
        var internalNodeId = nodeMapping.safeToMappedNodeId(neoNodeId);
        return internalNodeId == IdMapping.NOT_FOUND || nodeMapping
            .nodeLabels(internalNodeId)
            .stream()
            .noneMatch(filteredNodeLabels::contains);
    }

    private GraphStoreValidation() {}
}
