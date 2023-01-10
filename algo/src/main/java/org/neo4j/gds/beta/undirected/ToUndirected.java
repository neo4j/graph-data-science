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
package org.neo4j.gds.beta.undirected;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilderBuilder;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ToUndirected extends Algorithm<SingleTypeRelationships> {
    private final GraphStore graphStore;
    private final ToUndirectedConfig config;
    private final ExecutorService executorService;

    protected ToUndirected(
        GraphStore graphStore,
        ToUndirectedConfig config,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        super(progressTracker);

        this.graphStore = graphStore;
        this.config = config;
        this.executorService = executorService;
    }

    @Override
    public SingleTypeRelationships compute() {
        progressTracker.beginSubTask();

        RelationshipType fromRelationshipType = RelationshipType.of(config.relationshipType());

        var propertySchemas = graphStore
            .schema()
            .relationshipSchema()
            .propertySchemasFor(fromRelationshipType);
        var propertyKeys = propertySchemas.stream().map(PropertySchema::key).collect(Collectors.toList());

        var relationshipsBuilder = initializeRelationshipsBuilder(propertySchemas);

        var tasks = createTasks(fromRelationshipType, propertyKeys, relationshipsBuilder);

        progressTracker.beginSubTask();

        RunWithConcurrency.
            builder()
            .tasks(tasks)
            .concurrency(config.concurrency())
            .executor(executorService)
            .terminationFlag(terminationFlag)
            .build()
            .run();

        progressTracker.endSubTask();

        progressTracker.beginSubTask();
        var relationships = relationshipsBuilder.build();
        progressTracker.endSubTask();

        progressTracker.endSubTask();

        return relationships;
    }

    @NotNull
    private RelationshipsBuilder initializeRelationshipsBuilder(List<RelationshipPropertySchema> propertySchemas) {
        RelationshipsBuilderBuilder relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
            .concurrency(config.concurrency())
            .nodes(graphStore.nodes())
            .executorService(executorService)
            .orientation(Orientation.UNDIRECTED)
            .validateRelationships(false);

        propertySchemas.forEach(propertySchema ->
            relationshipsBuilderBuilder.addPropertyConfig(
                propertySchema.key(),
                propertySchema.aggregation(),
                propertySchema.defaultValue(),
                propertySchema.state()
            )
        );

        return relationshipsBuilderBuilder.build();
    }

    @NotNull
    private List<Runnable> createTasks(
        RelationshipType fromRelationshipType,
        List<String> propertyKeys,
        RelationshipsBuilder relationshipsBuilder
    ) {
        Function<DegreePartition, Runnable> taskCreator;
        if (propertyKeys.size() == 1) {
            Graph graph = graphStore.getGraph(fromRelationshipType, Optional.of(propertyKeys.get(0)));

            taskCreator = (partition) -> new ToUndirectedTaskWithSingleProperty(
                relationshipsBuilder,
                graph.concurrentCopy(),
                partition,
                progressTracker
            );
        }
        else {
            CompositeRelationshipIterator relationshipIterator = graphStore.getCompositeRelationshipIterator(
                fromRelationshipType,
                propertyKeys
            );

            taskCreator = (partition) -> new ToUndirectedTaskWithMultipleProperties(
                relationshipsBuilder,
                relationshipIterator.concurrentCopy(),
                partition,
                progressTracker
            );
        }

        return PartitionUtils.degreePartition(
            graphStore.getGraph(fromRelationshipType),
            config.concurrency(),
            taskCreator,
            Optional.empty()
        );
    }

    @Override
    public void release() {

    }

    private static final class ToUndirectedTaskWithSingleProperty implements Runnable {

        private final RelationshipsBuilder relationshipsBuilder;
        private final RelationshipIterator relationshipIterator;
        private final DegreePartition partition;
        private final ProgressTracker progressTracker;

        private ToUndirectedTaskWithSingleProperty(
            RelationshipsBuilder relationshipsBuilder,
            RelationshipIterator relationshipIterator,
            DegreePartition partition,
            ProgressTracker progressTracker
        ) {
            this.relationshipsBuilder = relationshipsBuilder;
            this.relationshipIterator = relationshipIterator;
            this.partition = partition;
            this.progressTracker = progressTracker;
        }

        @Override
        public void run() {
            for (long i = partition.startNode(); i < partition.startNode() + partition.nodeCount(); i++) {
                relationshipIterator.forEachRelationship(i, 0.0D, (source, target, property) -> {
                    relationshipsBuilder.addFromInternal(target, source, property);
                    return true;
                });
                progressTracker.logProgress();
            }
        }
    }

    private static final class ToUndirectedTaskWithMultipleProperties implements Runnable {

        private final RelationshipsBuilder relationshipsBuilder;
        private final CompositeRelationshipIterator relationshipIterator;
        private final DegreePartition partition;
        private final ProgressTracker progressTracker;

        private ToUndirectedTaskWithMultipleProperties(
            RelationshipsBuilder relationshipsBuilder,
            CompositeRelationshipIterator relationshipIterator,
            DegreePartition partition,
            ProgressTracker progressTracker
        ) {
            this.relationshipsBuilder = relationshipsBuilder;
            this.relationshipIterator = relationshipIterator;
            this.partition = partition;
            this.progressTracker = progressTracker;
        }

        @Override
        public void run() {
            for (long i = partition.startNode(); i < partition.startNode() + partition.nodeCount(); i++) {
                relationshipIterator.forEachRelationship(i, (source, target, properties) -> {
                    relationshipsBuilder.addFromInternal(target, source, properties);
                    return true;
                });
                progressTracker.logProgress();
            }
        }
    }
}
