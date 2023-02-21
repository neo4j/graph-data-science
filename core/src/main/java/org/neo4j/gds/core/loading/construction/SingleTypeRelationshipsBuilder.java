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
package org.neo4j.gds.core.loading.construction;

import org.immutables.builder.Builder;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.ImmutableProperties;
import org.neo4j.gds.api.ImmutableRelationshipProperty;
import org.neo4j.gds.api.ImmutableTopology;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.compress.AdjacencyListsWithProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.ImmutableRelationshipPropertySchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchemaEntry;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.AdjacencyBuffer;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.gds.core.loading.SingleTypeRelationships;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class SingleTypeRelationshipsBuilder {
    final PartialIdMap idMap;
    final int bufferSize;
    final RelationshipType relationshipType;
    final List<GraphFactory.PropertyConfig> propertyConfigs;

    final boolean isMultiGraph;
    final boolean loadRelationshipProperty;
    final Direction direction;

    private final ExecutorService executorService;
    private final int concurrency;

    @Builder.Factory
    static SingleTypeRelationshipsBuilder singleTypeRelationshipsBuilder(
        PartialIdMap idMap,
        SingleTypeRelationshipImporter importer,
        Optional<SingleTypeRelationshipImporter> inverseImporter,
        int bufferSize,
        RelationshipType relationshipType,
        List<GraphFactory.PropertyConfig> propertyConfigs,
        boolean isMultiGraph,
        boolean loadRelationshipProperty,
        Direction direction,
        ExecutorService executorService,
        int concurrency
    ) {
        return inverseImporter.isPresent() ?
            new Indexed(
                idMap,
                importer,
                inverseImporter.get(),
                bufferSize,
                relationshipType,
                propertyConfigs,
                isMultiGraph,
                loadRelationshipProperty,
                direction,
                executorService,
                concurrency
            ) :
            new NonIndexed(
                idMap,
                importer,
                bufferSize,
                relationshipType,
                propertyConfigs,
                isMultiGraph,
                loadRelationshipProperty,
                direction,
                executorService,
                concurrency
            );
    }

    SingleTypeRelationshipsBuilder(
        PartialIdMap idMap,
        int bufferSize,
        RelationshipType relationshipType,
        List<GraphFactory.PropertyConfig> propertyConfigs,
        boolean isMultiGraph,
        boolean loadRelationshipProperty,
        Direction direction,
        ExecutorService executorService,
        int concurrency
    ) {
        this.idMap = idMap;
        this.bufferSize = bufferSize;
        this.relationshipType = relationshipType;
        this.propertyConfigs = propertyConfigs;
        this.isMultiGraph = isMultiGraph;
        this.loadRelationshipProperty = loadRelationshipProperty;
        this.direction = direction;
        this.executorService = executorService;
        this.concurrency = concurrency;
    }

    abstract ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder();

    abstract Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks(
        Optional<AdjacencyCompressor.ValueMapper> mapper,
        Optional<LongConsumer> drainCountConsumer
    );

    abstract SingleTypeRelationships singleTypeRelationshipImportResult();

    PartialIdMap partialIdMap() {
        return idMap;
    }

    SingleTypeRelationships build(
        Optional<AdjacencyCompressor.ValueMapper> mapper,
        Optional<LongConsumer> drainCountConsumer
    ) {
        var adjacencyListBuilderTasks = adjacencyListBuilderTasks(mapper, drainCountConsumer);

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(adjacencyListBuilderTasks)
            .executor(executorService)
            .run();

        return singleTypeRelationshipImportResult();
    }

    MutableRelationshipSchemaEntry relationshipSchemaEntry(Optional<RelationshipPropertyStore> properties) {
        var entry = new MutableRelationshipSchemaEntry(
            relationshipType,
            direction
        );

        properties.ifPresent(relationshipPropertyStore -> relationshipPropertyStore
            .relationshipProperties()
            .forEach((propertyKey, relationshipProperty) -> entry.addProperty(
                propertyKey,
                RelationshipPropertySchema.of(propertyKey,
                    relationshipProperty.valueType(),
                    relationshipProperty.defaultValue(),
                    relationshipProperty.propertyState(),
                    relationshipProperty.aggregation()
                )
            ))
        );

        return entry;
    }

    RelationshipPropertyStore relationshipPropertyStore(AdjacencyListsWithProperties adjacencyListsWithProperties) {
        var propertyStoreBuilder = RelationshipPropertyStore.builder();

        var properties = adjacencyListsWithProperties.properties();
        var relationshipCount = adjacencyListsWithProperties.relationshipCount();

        for (int propertyKeyId = 0; propertyKeyId < this.propertyConfigs.size(); propertyKeyId++) {
            var propertyConfig = this.propertyConfigs.get(propertyKeyId);

            var propertyValues = ImmutableProperties.builder()
                .propertiesList(properties.get(propertyKeyId))
                .defaultPropertyValue(DefaultValue.DOUBLE_DEFAULT_FALLBACK)
                .elementCount(relationshipCount)
                .build();

            var relationshipPropertySchema = ImmutableRelationshipPropertySchema.builder()
                .key(propertyConfig.propertyKey())
                .aggregation(propertyConfig.aggregation())
                .valueType(ValueType.DOUBLE)
                .defaultValue(propertyConfig.defaultValue())
                .state(propertyConfig.propertyState())
                .build();

            var relationshipProperty = ImmutableRelationshipProperty.builder()
                .values(propertyValues)
                .propertySchema(relationshipPropertySchema)
                .build();

            propertyStoreBuilder.putRelationshipProperty(propertyConfig.propertyKey(), relationshipProperty);
        }

        return propertyStoreBuilder.build();
    }


    static class NonIndexed extends SingleTypeRelationshipsBuilder {

        private final SingleTypeRelationshipImporter importer;

        NonIndexed(
            PartialIdMap idMap,
            SingleTypeRelationshipImporter importer,
            int bufferSize,
            RelationshipType relationshipType,
            List<GraphFactory.PropertyConfig> propertyConfigs,
            boolean isMultiGraph,
            boolean loadRelationshipProperty,
            Direction direction,
            ExecutorService executorService,
            int concurrency
        ) {
            super(
                idMap,
                bufferSize,
                relationshipType,
                propertyConfigs,
                isMultiGraph,
                loadRelationshipProperty,
                direction,
                executorService,
                concurrency
            );
            this.importer = importer;
        }

        @Override
        ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder() {
            return new ThreadLocalRelationshipsBuilder.NonIndexed(idMap, importer, bufferSize, propertyConfigs.size());
        }

        @Override
        Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks(
            Optional<AdjacencyCompressor.ValueMapper> mapper,
            Optional<LongConsumer> drainCountConsumer
        ) {
            return importer.adjacencyListBuilderTasks(mapper, drainCountConsumer);
        }

        @Override
        SingleTypeRelationships singleTypeRelationshipImportResult() {
            var adjacencyListsWithProperties = importer.build();
            var adjacencyList = adjacencyListsWithProperties.adjacency();
            var relationshipCount = adjacencyListsWithProperties.relationshipCount();

            var topology = ImmutableTopology.builder()
                .isMultiGraph(isMultiGraph)
                .adjacencyList(adjacencyList)
                .elementCount(relationshipCount)
                .build();

            var singleRelationshipTypeImportResultBuilder = SingleTypeRelationships.builder().topology(topology);

            RelationshipPropertyStore properties = null;
            if (loadRelationshipProperty) {
                properties = relationshipPropertyStore(adjacencyListsWithProperties);
                singleRelationshipTypeImportResultBuilder.properties(properties);
            }

            singleRelationshipTypeImportResultBuilder
                .relationshipSchemaEntry(relationshipSchemaEntry(Optional.ofNullable(properties)));

            return singleRelationshipTypeImportResultBuilder.build();
        }
    }

    static class Indexed extends SingleTypeRelationshipsBuilder {

        private final SingleTypeRelationshipImporter forwardImporter;
        private final SingleTypeRelationshipImporter inverseImporter;

        Indexed(
            PartialIdMap idMap,
            SingleTypeRelationshipImporter forwardImporter,
            SingleTypeRelationshipImporter inverseImporter,
            int bufferSize,
            RelationshipType relationshipType,
            List<GraphFactory.PropertyConfig> propertyConfigs,
            boolean isMultiGraph,
            boolean loadRelationshipProperty,
            Direction direction,
            ExecutorService executorService,
            int concurrency
        ) {
            super(
                idMap,
                bufferSize,
                relationshipType,
                propertyConfigs,
                isMultiGraph,
                loadRelationshipProperty,
                direction,
                executorService,
                concurrency
            );
            this.forwardImporter = forwardImporter;
            this.inverseImporter = inverseImporter;
        }

        @Override
        ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder() {
            return new ThreadLocalRelationshipsBuilder.Indexed(
                new ThreadLocalRelationshipsBuilder.NonIndexed(idMap, forwardImporter, bufferSize, propertyConfigs.size()),
                new ThreadLocalRelationshipsBuilder.NonIndexed(idMap, inverseImporter, bufferSize, propertyConfigs.size())
            );
        }

        @Override
        Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks(
            Optional<AdjacencyCompressor.ValueMapper> mapper,
            Optional<LongConsumer> drainCountConsumer
        ) {
            var forwardTasks = forwardImporter.adjacencyListBuilderTasks(mapper, drainCountConsumer);
            var reverseTasks = inverseImporter.adjacencyListBuilderTasks(mapper, drainCountConsumer);

            return Stream.concat(forwardTasks.stream(), reverseTasks.stream()).collect(Collectors.toList());
        }

        @Override
        SingleTypeRelationships singleTypeRelationshipImportResult() {
            var forwardListWithProperties = forwardImporter.build();
            var inverseListWithProperties = inverseImporter.build();
            var forwardAdjacencyList = forwardListWithProperties.adjacency();
            var inverseAdjacencyList = inverseListWithProperties.adjacency();

            var relationshipCount = forwardListWithProperties.relationshipCount();

            var forwardTopology = ImmutableTopology.builder()
                .isMultiGraph(isMultiGraph)
                .adjacencyList(forwardAdjacencyList)
                .elementCount(relationshipCount)
                .build();

            var inverseTopology = ImmutableTopology.builder()
                .from(forwardTopology)
                .adjacencyList(inverseAdjacencyList)
                .build();

            var singleRelationshipTypeImportResultBuilder = SingleTypeRelationships.builder()
                .topology(forwardTopology)
                .inverseTopology(inverseTopology);

            RelationshipPropertyStore forwardProperties = null;
            if (loadRelationshipProperty) {
                forwardProperties = relationshipPropertyStore(forwardListWithProperties);
                var inverseProperties = relationshipPropertyStore(inverseListWithProperties);
                singleRelationshipTypeImportResultBuilder.properties(forwardProperties).inverseProperties(inverseProperties);
            }

            singleRelationshipTypeImportResultBuilder
                .relationshipSchemaEntry(relationshipSchemaEntry(Optional.ofNullable(forwardProperties)));

            return singleRelationshipTypeImportResultBuilder.build();
        }
    }
}
