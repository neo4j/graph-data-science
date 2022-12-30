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
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.ImmutableProperties;
import org.neo4j.gds.api.ImmutableRelationships;
import org.neo4j.gds.api.ImmutableTopology;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.AdjacencyBuffer;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;

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
    final int[] propertyKeyIds;

    final boolean isMultiGraph;
    final boolean loadRelationshipProperty;
    final Direction direction;

    private final ExecutorService executorService;
    private final int concurrency;

    @Builder.Factory
    static SingleTypeRelationshipsBuilder singleTypeRelationshipsBuilder(
        PartialIdMap idMap,
        SingleTypeRelationshipImporter importer,
        Optional<SingleTypeRelationshipImporter> reverseImporter,
        int bufferSize,
        int[] propertyKeyIds,
        boolean isMultiGraph,
        boolean loadRelationshipProperty,
        Direction direction,
        ExecutorService executorService,
        int concurrency
    ) {
        return reverseImporter.isPresent() ?
            new Indexed(
                idMap,
                importer,
                reverseImporter.get(),
                bufferSize,
                propertyKeyIds,
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
                propertyKeyIds,
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
        int[] propertyKeyIds,
        boolean isMultiGraph,
        boolean loadRelationshipProperty,
        Direction direction,
        ExecutorService executorService,
        int concurrency
    ) {
        this.idMap = idMap;
        this.bufferSize = bufferSize;
        this.propertyKeyIds = propertyKeyIds;
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

    abstract List<RelationshipsAndDirection> relationshipsAndDirections();

    PartialIdMap partialIdMap() {
        return idMap;
    }

    List<RelationshipsAndDirection> buildAll(
        Optional<AdjacencyCompressor.ValueMapper> mapper,
        Optional<LongConsumer> drainCountConsumer
    ) {
        var adjacencyListBuilderTasks = adjacencyListBuilderTasks(mapper, drainCountConsumer);

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(adjacencyListBuilderTasks)
            .executor(executorService)
            .run();

        return relationshipsAndDirections();
    }

    static class NonIndexed extends SingleTypeRelationshipsBuilder {

        private final SingleTypeRelationshipImporter importer;

        NonIndexed(
            PartialIdMap idMap,
            SingleTypeRelationshipImporter importer,
            int bufferSize,
            int[] propertyKeyIds,
            boolean isMultiGraph,
            boolean loadRelationshipProperty,
            Direction direction,
            ExecutorService executorService,
            int concurrency
        ) {
            super(
                idMap,
                bufferSize,
                propertyKeyIds,
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
            return new ThreadLocalRelationshipsBuilder.NonIndexed(idMap, importer, bufferSize, propertyKeyIds);
        }

        @Override
        Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks(
            Optional<AdjacencyCompressor.ValueMapper> mapper,
            Optional<LongConsumer> drainCountConsumer
        ) {
            return importer.adjacencyListBuilderTasks(mapper, drainCountConsumer);
        }

        @Override
        List<RelationshipsAndDirection> relationshipsAndDirections() {
            var adjacencyListsWithProperties = importer.build();
            var adjacencyList = adjacencyListsWithProperties.adjacency();
            var relationshipCount = adjacencyListsWithProperties.relationshipCount();

            var topology = ImmutableTopology.builder()
                .isMultiGraph(isMultiGraph)
                .adjacencyList(adjacencyList)
                .elementCount(relationshipCount)
                .build();

            if (loadRelationshipProperty) {
                return adjacencyListsWithProperties.properties().stream().map(compressedProperties ->
                    {
                        var properties = ImmutableProperties.builder()
                            .propertiesList(compressedProperties)
                            .defaultPropertyValue(DefaultValue.DOUBLE_DEFAULT_FALLBACK)
                            .elementCount(relationshipCount)
                            .build();
                        var relationships = ImmutableRelationships.builder()
                            .topology(topology)
                            .properties(properties)
                            .build();
                        return RelationshipsAndDirection.of(relationships, direction);
                    }
                ).collect(Collectors.toList());
            } else {
                var relationships = ImmutableRelationships.builder().topology(topology).build();
                return List.of(RelationshipsAndDirection.of(relationships, direction));
            }
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
            int[] propertyKeyIds,
            boolean isMultiGraph,
            boolean loadRelationshipProperty,
            Direction direction,
            ExecutorService executorService,
            int concurrency
        ) {
            super(
                idMap,
                bufferSize,
                propertyKeyIds,
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
                new ThreadLocalRelationshipsBuilder.NonIndexed(idMap, forwardImporter, bufferSize, propertyKeyIds),
                new ThreadLocalRelationshipsBuilder.NonIndexed(idMap, inverseImporter, bufferSize, propertyKeyIds)
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
        List<RelationshipsAndDirection> relationshipsAndDirections() {
            var forwardListWithProperties = forwardImporter.build();
            var inverseListWithProperties = inverseImporter.build();
            var forwardAdjacencyList = forwardListWithProperties.adjacency();
            var inverseAdjacencyList = inverseListWithProperties.adjacency();

            var relationshipCount = forwardListWithProperties.relationshipCount();

            var topology = ImmutableTopology.builder()
                .isMultiGraph(isMultiGraph)
                .adjacencyList(forwardAdjacencyList)
                .inverseAdjacencyList(inverseAdjacencyList)
                .elementCount(relationshipCount)
                .build();

            if (loadRelationshipProperty) {
                // TODO: properties for inverse
                return forwardListWithProperties.properties().stream().map(compressedProperties -> {
                        var properties = ImmutableProperties.builder()
                            .propertiesList(compressedProperties)
                            .defaultPropertyValue(DefaultValue.DOUBLE_DEFAULT_FALLBACK)
                            .elementCount(relationshipCount)
                            .build();
                        var relationships = ImmutableRelationships.builder()
                            .topology(topology)
                            .properties(properties)
                            .build();
                        return RelationshipsAndDirection.of(relationships, direction);
                    }
                ).collect(Collectors.toList());
            } else {
                var relationships = ImmutableRelationships.builder().topology(topology).build();
                return List.of(RelationshipsAndDirection.of(relationships, direction));
            }
        }
    }
}
