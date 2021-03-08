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
package org.neo4j.graphalgo.beta.generator;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.UnionNodeProperties;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public final class RandomGraphGenerator {

    private final AllocationTracker allocationTracker;
    private final long nodeCount;
    private final long averageDegree;
    private final Random random;
    private final RelationshipDistribution relationshipDistribution;
    private final Aggregation aggregation;
    private final Orientation orientation;
    private final AllowSelfLoops allowSelfLoops;

    private final Optional<NodeLabelProducer> maybeNodeLabelProducer;
    private final Optional<PropertyProducer> maybeRelationshipPropertyProducer;
    private final Map<NodeLabel, Set<PropertyProducer>> nodePropertyProducers;

    public RandomGraphGenerator(
        long nodeCount,
        long averageDegree,
        RelationshipDistribution relationshipDistribution,
        @Nullable Long seed,
        Optional<NodeLabelProducer> maybeNodeLabelProducer,
        Map<NodeLabel, Set<PropertyProducer>> nodePropertyProducers,
        Optional<PropertyProducer> maybeRelationshipPropertyProducer,
        Aggregation aggregation,
        Orientation orientation,
        AllowSelfLoops allowSelfLoops,
        AllocationTracker allocationTracker
    ) {
        this.relationshipDistribution = relationshipDistribution;
        this.maybeNodeLabelProducer = maybeNodeLabelProducer;
        this.nodePropertyProducers = nodePropertyProducers;
        this.maybeRelationshipPropertyProducer = maybeRelationshipPropertyProducer;
        this.allocationTracker = allocationTracker;
        this.nodeCount = nodeCount;
        this.averageDegree = averageDegree;
        this.aggregation = aggregation;
        this.orientation = orientation;
        this.allowSelfLoops = allowSelfLoops;
        this.random = new Random();
        if (seed != null) {
            this.random.setSeed(seed);
        } else {
            this.random.setSeed(1);
        }
    }

    public static RandomGraphGeneratorBuilder builder() {
        return new RandomGraphGeneratorBuilder();
    }

    public HugeGraph generate() {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(nodeCount)
            .hasLabelInformation(maybeNodeLabelProducer.isPresent())
            .tracker(allocationTracker)
            .build();

        if (maybeNodeLabelProducer.isPresent()) {
            generateNodes(nodesBuilder, maybeNodeLabelProducer.get());
        } else {
            generateNodes(nodesBuilder);
        }

        NodeMapping idMap = nodesBuilder.build();
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(orientation)
            .tracker(allocationTracker)
            .addAllPropertyConfigs(maybeRelationshipPropertyProducer.isPresent()
                ? List.of(GraphFactory.PropertyConfig.of(aggregation, DefaultValue.forDouble()))
                : List.of()
            )
            .aggregation(aggregation)
            .build();

        generateRelationships(relationshipsBuilder);

        if (!nodePropertyProducers.isEmpty()) {
            var nodeProperties = generateNodeProperties(idMap);

            return GraphFactory.create(
                idMap,
                nodeProperties.nodeSchema(),
                nodeProperties.nodeProperties(),
                relationshipsBuilder.build(),
                allocationTracker
            );
        } else {
            return GraphFactory.create(
                idMap,
                relationshipsBuilder.build(),
                allocationTracker
            );
        }
    }

    public RelationshipDistribution getRelationshipDistribution() {
        return relationshipDistribution;
    }

    public Optional<PropertyProducer> getMaybeRelationshipPropertyProducer() {
        return maybeRelationshipPropertyProducer;
    }

    private void generateNodes(NodesBuilder nodesBuilder, NodeLabelProducer nodeLabelProducer) {
        for (long i = 0; i < nodeCount; i++) {
            nodesBuilder.addNode(i, nodeLabelProducer.labels(i));
        }
    }

    private void generateNodes(NodesBuilder nodesBuilder) {
        for (long i = 0; i < nodeCount; i++) {
            nodesBuilder.addNode(i);
        }
    }

    private void generateRelationships(RelationshipsBuilder relationshipsImporter) {
        LongUnaryOperator degreeProducer = relationshipDistribution.degreeProducer(nodeCount, averageDegree, random);
        LongUnaryOperator relationshipProducer = relationshipDistribution.relationshipProducer(
            nodeCount,
            averageDegree,
            random
        );
        PropertyProducer relationshipPropertyProducer = maybeRelationshipPropertyProducer.orElse(new PropertyProducer.EmptyPropertyProducer());

        long degree, targetId;
        double property;

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            degree = degreeProducer.applyAsLong(nodeId);

            for (int j = 0; j < degree; j++) {
                targetId = relationshipProducer.applyAsLong(nodeId);
                if (!allowSelfLoops.value()) {
                    while (targetId == nodeId) {
                        targetId = relationshipProducer.applyAsLong(nodeId);
                    }
                }
                assert (targetId < nodeCount);
                property = relationshipPropertyProducer.getPropertyValue(random);
                // For POWER_LAW, we generate a normal distributed out-degree value
                // and connect to nodes where the target is power-law-distributed.
                // In order to have the out degree follow a power-law distribution,
                // we have to swap the relationship.
                if (relationshipDistribution == RelationshipDistribution.POWER_LAW) {
                    relationshipsImporter.addFromInternal(targetId, nodeId, property);
                } else {
                    relationshipsImporter.addFromInternal(nodeId, targetId, property);
                }
            }
        }
    }

    @ValueClass
    interface NodePropertiesAndSchema {
        NodeSchema nodeSchema();

        Map<String, NodeProperties> nodeProperties();
    }

    private NodePropertiesAndSchema generateNodeProperties(NodeMapping idMap) {
        var propertyToLabels = new HashMap<String, List<NodeLabel>>();
        var propertyToArray = new HashMap<String, HugeDoubleArray>();

        nodePropertyProducers.forEach((nodeLabel, propertyProducers) ->
            propertyProducers.forEach(propertyProducer -> {
                    propertyToLabels
                        .computeIfAbsent(propertyProducer.getPropertyName(), ignore -> new ArrayList<>())
                        .add(nodeLabel);
                    propertyToArray.put(
                        propertyProducer.getPropertyName(),
                        HugeDoubleArray.newArray(nodeCount, allocationTracker)
                    );
                }
            ));

        // Extract all property producers
        var propertyProducers = nodePropertyProducers
            .values()
            .stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

        // Fill property arrays
        for (var propertyProducer : propertyProducers) {
            var array = propertyToArray.get(propertyProducer.getPropertyName());
            for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
               array.set(nodeId, propertyProducer.getPropertyValue(random));
            }
        }

        // Construct union node properties
        Map<String, NodeProperties> unionProperties = propertyToLabels.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> {
                var propertyKey = entry.getKey();
                var nodeLabels = entry.getValue();
                var nodeProperties = propertyToArray.get(propertyKey).asNodeProperties();
                var nodeLabelToProperties = nodeLabels
                    .stream()
                    .collect(Collectors.toMap(nodeLabel -> nodeLabel, nodeLabel -> (NodeProperties) nodeProperties));

                return new UnionNodeProperties(idMap, nodeLabelToProperties);
            }
        ));

        // Create a corresponding node schema
        var nodeSchemaBuilder = NodeSchema.builder();
        unionProperties.forEach((propertyKey, property) -> {
            propertyToLabels.get(propertyKey).forEach(nodeLabel ->
                nodeSchemaBuilder.addProperty(
                    nodeLabel,
                    propertyKey,
                    property.valueType()
                ));
        });

        return ImmutableNodePropertiesAndSchema.builder()
            .nodeProperties(unionProperties)
            .nodeSchema(nodeSchemaBuilder.build())
            .build();
    }
}
