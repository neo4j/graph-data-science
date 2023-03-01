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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.indexInverse.InverseRelationshipsAlgorithmFactory;
import org.neo4j.gds.beta.indexInverse.InverseRelationshipsConfigImpl;
import org.neo4j.gds.beta.pregel.PregelConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.validation.AfterLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PregelBaseProc {

    public static <CONFIG extends PregelConfig> ValidationConfiguration<CONFIG> ensureIndexValidation(
        Log log, TaskRegistryFactory taskRegistryFactory
    ) {
        return new ValidationConfiguration<>() {
            @Override
            public List<AfterLoadValidation<CONFIG>> afterLoadValidations() {
                return List.of(
                    (graphStore, graphProjectConfig, config) -> ensureDirectedRelationships(
                        graphStore, config.internalRelationshipTypes(graphStore)
                    ),
                    (graphStore, graphProjectConfig, config) -> ensureInverseIndexesExist(graphStore,
                        config.internalRelationshipTypes(graphStore),
                        config.concurrency(),
                        log,
                        taskRegistryFactory
                    )
                );
            }
        };
    }

    static void ensureInverseIndexesExist(
        GraphStore graphStore,
        Collection<RelationshipType> relationshipTypes,
        int concurrency,
        Log log,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var relationshipTypesWithoutIndex = relationshipTypes
            .stream()
            .filter(relType -> !graphStore.inverseIndexedRelationshipTypes().contains(relType))
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (relationshipTypesWithoutIndex.isEmpty()) {
            return;
        }

        var inverseConfig = InverseRelationshipsConfigImpl
            .builder()
            .concurrency(concurrency)
            .relationshipTypes(relationshipTypesWithoutIndex)
            .build();

        new InverseRelationshipsAlgorithmFactory()
            .build(graphStore, inverseConfig, log, taskRegistryFactory)
            .compute()
            .forEach((relationshipType, inverseIndex) -> graphStore.addInverseIndex(
                relationshipType,
                inverseIndex.topology(),
                inverseIndex.properties()
            ));
    }

    static void ensureDirectedRelationships(GraphStore graphStore, Collection<RelationshipType> relationshipTypes) {
        var relationshipSchema = graphStore.schema().relationshipSchema();
        var undirectedTypes = relationshipTypes
            .stream()
            .filter(relationshipSchema::isUndirected)
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!undirectedTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                Locale.US,
                "This algorithm requires a directed graph, but the following configured relationship types are undirected: %s.",
                StringJoining.join(undirectedTypes)
            ));
        }
    }

    static <ALGO extends Algorithm<PregelResult>, CONFIG extends PregelConfig> List<NodeProperty> nodeProperties(
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult, String propertyPrefix
    ) {
        var compositeNodeValue = computationResult.result().nodeValues();
        var schema = compositeNodeValue.schema();
        // TODO change this to generic prefix setting

        return schema
            .elements()
            .stream()
            .filter(element -> element.visibility() == PregelSchema.Visibility.PUBLIC)
            .map(element -> {
                var propertyKey = element.propertyKey();

                NodePropertyValues nodePropertyValues;
                switch (element.propertyType()) {
                    case LONG:
                        nodePropertyValues = compositeNodeValue.longProperties(propertyKey).asNodeProperties();
                        break;
                    case DOUBLE:
                        nodePropertyValues = compositeNodeValue.doubleProperties(propertyKey).asNodeProperties();
                        break;
                    case LONG_ARRAY:
                        nodePropertyValues = new HugeObjectArrayLongArrayPropertyValues(compositeNodeValue.longArrayProperties(propertyKey));
                        break;
                    case DOUBLE_ARRAY:
                        nodePropertyValues = new HugeObjectArrayDoubleArrayPropertyValues(compositeNodeValue.doubleArrayProperties(propertyKey));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported property type: " + element.propertyType());
                }

                return ImmutableNodeProperty.of(formatWithLocale("%s%s", propertyPrefix, propertyKey), nodePropertyValues);
            }).collect(Collectors.toList());
    }

    private PregelBaseProc() {}

    static class HugeObjectArrayLongArrayPropertyValues implements LongArrayNodePropertyValues {
        private final HugeObjectArray<long[]> longArrays;

        HugeObjectArrayLongArrayPropertyValues(HugeObjectArray<long[]> longArrays) {this.longArrays = longArrays;}

        @Override
        public long valuesStored() {
            return longArrays.size();
        }

        @Override
        public long[] longArrayValue(long nodeId) {
            return longArrays.get(nodeId);
        }
    }

    static class HugeObjectArrayDoubleArrayPropertyValues implements DoubleArrayNodePropertyValues {
        private final HugeObjectArray<double[]> doubleArrays;

        HugeObjectArrayDoubleArrayPropertyValues(HugeObjectArray<double[]> doubleArrays) {this.doubleArrays = doubleArrays;}

        @Override
        public long valuesStored() {
            return doubleArrays.size();
        }


        @Override
        public double[] doubleArrayValue(long nodeId) {
            return doubleArrays.get(nodeId);
        }
    }

}
