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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.ConfigurableSeedConfig;
import org.neo4j.graphalgo.config.FeaturePropertiesConfig;
import org.neo4j.graphalgo.config.MutatePropertyConfig;
import org.neo4j.graphalgo.config.MutateRelationshipConfig;
import org.neo4j.graphalgo.config.NodeWeightConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.config.SourceNodeConfig;
import org.neo4j.graphalgo.config.SourceNodesConfig;
import org.neo4j.graphalgo.config.TargetNodeConfig;
import org.neo4j.graphalgo.utils.StringJoining;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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
        if (config instanceof NodeWeightConfig) {
            validateNodeWeightProperty(graphStore, (NodeWeightConfig) config, filterLabels);
        }
        if (config instanceof RelationshipWeightConfig) {
            validateRelationshipWeightProperty(graphStore, config);
        }
        if (config instanceof MutatePropertyConfig) {
            validateMutateProperty(graphStore, filterLabels, (MutatePropertyConfig) config);
        }
        if (config instanceof MutateRelationshipConfig) {
            validateMutateRelationships(graphStore, (MutateRelationshipConfig) config);
        }
        if (config instanceof SourceNodesConfig) {
            validateSourceNodes(graphStore, (SourceNodesConfig) config);

        }
        if (config instanceof SourceNodeConfig) {
            validateSourceNode(graphStore, (SourceNodeConfig) config);
        }
        if (config instanceof TargetNodeConfig) {
            validateTargetNode(graphStore, (TargetNodeConfig) config);
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

    private static void validateNodeWeightProperty(GraphStore graphStore, NodeWeightConfig config, Collection<NodeLabel> filterLabels) {
        String weightProperty = config.nodeWeightProperty();
        if (weightProperty != null && !graphStore.hasNodeProperty(filterLabels, weightProperty)) {
            var labelsWithMissingProperty = filterLabels
                .stream()
                .filter(label -> !graphStore.nodePropertyKeys(label).contains(weightProperty))
                .map(NodeLabel::name)
                .collect(Collectors.toList());

            throw new IllegalArgumentException(formatWithLocale(
                "Node weight property `%s` is not present for all requested labels. Requested labels: %s. Labels without the property key: %s. Properties available on all requested labels: %s",
                weightProperty,
                StringJoining.join(filterLabels.stream().map(NodeLabel::name)),
                StringJoining.join(labelsWithMissingProperty),
                StringJoining.join(graphStore.nodePropertyKeys(filterLabels))
            ));
        }
    }

    private static void validateRelationshipWeightProperty(GraphStore graphStore, AlgoBaseConfig config) {
        String weightProperty = ((RelationshipWeightConfig) config).relationshipWeightProperty();
        Collection<RelationshipType> internalRelationshipTypes = config.internalRelationshipTypes(graphStore);
        if (weightProperty != null) {
            var relTypesWithoutProperty = typesWithoutRelationshipProperty(graphStore, internalRelationshipTypes, weightProperty);
            if (!relTypesWithoutProperty.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Relationship weight property `%s` not found in relationship types %s. Properties existing on all relationship types: %s",
                    weightProperty,
                    StringJoining.join(relTypesWithoutProperty.stream().map(RelationshipType::name)),
                    StringJoining.join(graphStore.relationshipPropertyKeys(internalRelationshipTypes))
                ));
            }
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

    private static void validateSourceNode(GraphStore graphStore, SourceNodeConfig config) {
        var sourceNodeId = config.sourceNode();

        if (graphStore.nodes().toMappedNodeId(sourceNodeId) == NodeMapping.NOT_FOUND) {
            throw new IllegalArgumentException(formatWithLocale(
                "Source node does not exist in the in-memory graph: `%d`",
                sourceNodeId
            ));
        }
    }

    private static void validateSourceNodes(GraphStore graphStore, SourceNodesConfig config) {
        var nodeMapping = graphStore.nodes();
        var missingNodes = config.sourceNodes().stream()
            .filter(nodeId -> nodeMapping.toMappedNodeId(nodeId) == NodeMapping.NOT_FOUND)
            .map(Object::toString)
            .collect(Collectors.toList());

        if (!missingNodes.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Source nodes do not exist in the in-memory graph: %s",
                StringJoining.join(missingNodes)
            ));
        }
    }

    private static void validateTargetNode(GraphStore graphStore, TargetNodeConfig config) {
        var targetNodeId = config.targetNode();

        if (graphStore.nodes().toMappedNodeId(targetNodeId) == NodeMapping.NOT_FOUND) {
            throw new IllegalArgumentException(formatWithLocale(
                "Target node does not exist in the in-memory graph: `%d`",
                targetNodeId
            ));
        }
    }

    private static Set<RelationshipType> typesWithoutRelationshipProperty(GraphStore graphStore, Collection<RelationshipType> relTypes, String propertyKey) {
        return relTypes.stream()
            .filter(relType -> !graphStore.hasRelationshipProperty(relType, propertyKey))
            .collect(Collectors.toSet());
    }

    private GraphStoreValidation() {}
}
