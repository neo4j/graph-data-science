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
package org.neo4j.gds.beta.generator;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.paged.HugeArray;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class RandomGraphGenerator {

    private final long nodeCount;
    private final long averageDegree;
    private final Random random;
    private final RelationshipType relationshipType;
    private final RelationshipDistribution relationshipDistribution;
    private final Aggregation aggregation;
    private final Direction direction;
    private final AllowSelfLoops allowSelfLoops;

    private final Optional<NodeLabelProducer> maybeNodeLabelProducer;
    private final Optional<PropertyProducer<double[]>> maybeRelationshipPropertyProducer;
    private final Map<NodeLabel, Set<PropertyProducer<?>>> nodePropertyProducers;

    RandomGraphGenerator(
        long nodeCount,
        long averageDegree,
        RelationshipType relationshipType,
        RelationshipDistribution relationshipDistribution,
        @Nullable Long seed,
        Optional<NodeLabelProducer> maybeNodeLabelProducer,
        Map<NodeLabel, Set<PropertyProducer<?>>> nodePropertyProducers,
        Optional<PropertyProducer<double[]>> maybeRelationshipPropertyProducer,
        Aggregation aggregation,
        Direction direction,
        AllowSelfLoops allowSelfLoops
    ) {
        this.relationshipType = relationshipType;
        this.relationshipDistribution = relationshipDistribution;
        this.maybeNodeLabelProducer = maybeNodeLabelProducer;
        this.nodePropertyProducers = nodePropertyProducers;
        this.maybeRelationshipPropertyProducer = maybeRelationshipPropertyProducer;
        this.nodeCount = nodeCount;
        this.averageDegree = averageDegree;
        this.aggregation = aggregation;
        this.direction = direction;
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
            .build();

        if (maybeNodeLabelProducer.isPresent()) {
            generateNodes(nodesBuilder, maybeNodeLabelProducer.get());
        } else {
            generateNodes(nodesBuilder);
        }

        var idMap = nodesBuilder.build().idMap();
        var nodePropertiesAndSchema = generateNodeProperties(idMap);

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(direction.toOrientation())
            .addAllPropertyConfigs(maybeRelationshipPropertyProducer
                .map(propertyProducer -> List.of(GraphFactory.PropertyConfig.of(
                    propertyProducer.getPropertyName(),
                    aggregation,
                    DefaultValue.forDouble()
                )))
                .orElseGet(List::of)
            )
            .aggregation(aggregation)
            .build();

        generateRelationships(relationshipsBuilder);

        var relationships = relationshipsBuilder.build();

        var graphSchema = GraphSchema.of(
            nodePropertiesAndSchema.nodeSchema(),
            relationships.relationshipSchema(relationshipType),
            Map.of()
        );

        return GraphFactory.create(graphSchema, idMap, nodePropertiesAndSchema.nodeProperties(), relationships);
    }

    public RelationshipDistribution getRelationshipDistribution() {
        return relationshipDistribution;
    }

    public Optional<PropertyProducer<double[]>> getMaybeRelationshipPropertyProducer() {
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
        PropertyProducer<double[]> relationshipPropertyProducer =
            maybeRelationshipPropertyProducer.orElseGet(PropertyProducer.EmptyPropertyProducer::new);

        long degree, targetId;
        double[] property = new double[1];

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
                relationshipPropertyProducer.setProperty(nodeId, property, 0, random);
                // For POWER_LAW, we generate a normal distributed out-degree value
                // and connect to nodes where the target is power-law-distributed.
                // In order to have the out degree follow a power-law distribution,
                // we have to swap the relationship.
                if (relationshipDistribution == RelationshipDistribution.POWER_LAW) {
                    relationshipsImporter.addFromInternal(targetId, nodeId, property[0]);
                } else {
                    relationshipsImporter.addFromInternal(nodeId, targetId, property[0]);
                }
            }
        }
    }

    @ValueClass
    interface NodePropertiesAndSchema {
        NodeSchema nodeSchema();

        Map<String, NodePropertyValues> nodeProperties();
    }

    private NodePropertiesAndSchema generateNodeProperties(IdMap idMap) {
        if (this.nodePropertyProducers.isEmpty()) {
            var nodeSchema = NodeSchema.empty();
            idMap.availableNodeLabels().forEach(nodeSchema::getOrCreateLabel);
            return ImmutableNodePropertiesAndSchema.builder()
                .nodeSchema(nodeSchema)
                .nodeProperties(Map.of())
                .build();
        }

        var propertyNameToLabels = new HashMap<String, List<NodeLabel>>();
        var propertyNameToProducers = new HashMap<String, PropertyProducer<?>>();

        this.nodePropertyProducers.forEach((nodeLabel, propertyProducers) -> {
            if (nodeLabel != NodeLabel.ALL_NODES && !idMap.availableNodeLabels().contains(nodeLabel)) {
                return;
            }

            propertyProducers.forEach(propertyProducer -> {
                // map property names to all labels for that property
                propertyNameToLabels
                    .computeIfAbsent(propertyProducer.getPropertyName(), ignore -> new ArrayList<>())
                    .add(nodeLabel);
                // group producers by property name
                propertyNameToProducers.merge(
                    propertyProducer.getPropertyName(),
                    propertyProducer,
                    (first, second) -> {
                        if (!first.equals(second)) {
                            throw new IllegalArgumentException(formatWithLocale(
                                "Duplicate node properties with name [%s]. The first property producer is [%s], the second one is [%s].",
                                first.getPropertyName(),
                                first,
                                second
                            ));
                        }
                        return first;
                    }
                );
                });
            }
        );

        Map<String, NodePropertyValues> generatedProperties = propertyNameToProducers.entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            entry -> {
                var nodeLabels = new HashSet<>(propertyNameToLabels.get(entry.getKey()));
                var nodes = idMap.nodeIterator(nodeLabels);
                return generateProperties(nodes, entry.getValue());
            }
        ));

        // Create a corresponding node schema
        var nodeSchema = NodeSchema.empty();
        generatedProperties.forEach((propertyKey, property) -> propertyNameToLabels
            .get(propertyKey)
            .forEach(nodeLabel -> {
                if (nodeLabel == NodeLabel.ALL_NODES) {
                    idMap
                        .availableNodeLabels()
                        .forEach(actualNodeLabel -> nodeSchema
                            .getOrCreateLabel(actualNodeLabel)
                            .addProperty(
                                propertyKey,
                                property.valueType()
                            )
                        );
                } else {
                    nodeSchema.getOrCreateLabel(nodeLabel).addProperty(propertyKey, property.valueType());
                }
            }));

        return ImmutableNodePropertiesAndSchema.builder()
            .nodeProperties(generatedProperties)
            .nodeSchema(nodeSchema)
            .build();
    }

    @SuppressWarnings("unchecked")
    private NodePropertyValues generateProperties(
        PrimitiveIterator.OfLong nodes,
        PropertyProducer<?> propertyProducer
    ) {
        switch (propertyProducer.propertyType()) {
            case LONG:
                var longValues = HugeLongArray.newArray(nodeCount);
                longValues.fill(DefaultValue.forLong().longValue());
                return generateProperties(
                    nodes,
                    longValues,
                    (PropertyProducer<long[]>) propertyProducer,
                    HugeLongArray::asNodeProperties
                );
            case DOUBLE:
                var doubleValues = HugeDoubleArray.newArray(nodeCount);
                doubleValues.fill(DefaultValue.forDouble().doubleValue());
                return generateProperties(
                    nodes,
                    doubleValues,
                    (PropertyProducer<double[]>) propertyProducer,
                    HugeDoubleArray::asNodeProperties
                );
            case DOUBLE_ARRAY:
                return generateProperties(
                    nodes,
                    HugeObjectArray.newArray(double[].class, nodeCount),
                    (PropertyProducer<double[][]>) propertyProducer,
                    HugeObjectArray::asNodeProperties
                );
            case FLOAT_ARRAY:
                return generateProperties(
                    nodes,
                    HugeObjectArray.newArray(float[].class, nodeCount),
                    (PropertyProducer<float[][]>) propertyProducer,
                    HugeObjectArray::asNodeProperties
                );
            case LONG_ARRAY:
                return generateProperties(
                    nodes,
                    HugeObjectArray.newArray(long[].class, nodeCount),
                    (PropertyProducer<long[][]>) propertyProducer,
                    HugeObjectArray::asNodeProperties
                );
            default:
                throw new UnsupportedOperationException("properties producer must return a known value type");
        }
    }

    private <T, A extends HugeArray<T, ?, A>> NodePropertyValues generateProperties(
        PrimitiveIterator.OfLong nodes,
        A values,
        PropertyProducer<T> propertyProducer,
        Function<A, NodePropertyValues> toProperties
    ) {
        var cursor = values.initCursor(values.newCursor());
        while (nodes.hasNext()) {
            var nodeId = nodes.nextLong();
            var i = seek(nodeId, cursor);
            propertyProducer.setProperty(nodeId, cursor.array, i, random);
        }
        return toProperties.apply(values);
    }

    private <T> int seek(long targetNode, HugeCursor<T> cursor) {
        while (cursor.base < targetNode && cursor.base + cursor.limit < targetNode) {
            if (!cursor.next()) {
                throw new IllegalStateException("");
            }
        }
        return Math.toIntExact(targetNode - cursor.base);
    }


}
