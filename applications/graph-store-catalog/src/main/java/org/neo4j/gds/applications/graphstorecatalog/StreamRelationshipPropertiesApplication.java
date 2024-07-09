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
package org.neo4j.gds.applications.graphstorecatalog;

import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class StreamRelationshipPropertiesApplication {
    private final Log log;

    public StreamRelationshipPropertiesApplication(Log log) {
        this.log = log;
    }

    <T> Stream<T> compute(
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphStore graphStore,
        GraphStreamRelationshipPropertiesConfig configuration,
        boolean usesPropertyNameColumn,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        Collection<RelationshipType> validRelationshipTypes = configuration.validRelationshipTypes(graphStore);

        var relationshipPropertyKeysAndValues = validRelationshipTypes
            .stream()
            .flatMap(relType -> configuration.relationshipProperties()
                .stream()
                .filter(propertyKey -> graphStore.hasRelationshipProperty(relType, propertyKey))
                .map(propertyKey -> Triple.of(
                    relType,
                    propertyKey,
                    graphStore.getGraph(relType, Optional.of(propertyKey))
                ))
            )
            .collect(Collectors.toList());

        var task = Tasks.leaf(
            "Graph :: RelationshipProperties :: Stream",
            graphStore.nodeCount() * relationshipPropertyKeysAndValues.size()
        );

        var taskProgressTracker = new TaskProgressTracker(
            task,
            log,
            configuration.concurrency(),
            new JobId(),
            taskRegistryFactory,
            userLogRegistryFactory
        );

        return computeWithProgressTracking(
            graphStore,
            usesPropertyNameColumn,
            outputMarshaller,
            taskProgressTracker,
            relationshipPropertyKeysAndValues
        );
    }

    <T> Stream<T> computeWithProgressTracking(
        GraphStore graphStore,
        boolean usesPropertyNameColumn,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller,
        ProgressTracker taskProgressTracker,
        List<Triple<RelationshipType, String, Graph>> relationshipPropertyKeysAndValues
    ) {
        taskProgressTracker.beginSubTask();

        return computeRelationshipPropertyStream(
            graphStore,
            usesPropertyNameColumn,
            outputMarshaller,
            relationshipPropertyKeysAndValues
        ).onClose(taskProgressTracker::endSubTask);
    }

    <T> Stream<T> computeRelationshipPropertyStream(
        GraphStore graphStore,
        boolean usesPropertyNameColumn,
        GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller,
        List<Triple<RelationshipType, String, Graph>> relationshipPropertyKeysAndValues
    ) {
        return LongStream
            .range(0, graphStore.nodeCount())
            .mapToObj(nodeId -> relationshipPropertyKeysAndValues.stream().flatMap(relTypeAndPropertyKeyAndValues -> {
                ValueType valueType = graphStore.relationshipPropertyType(relTypeAndPropertyKeyAndValues.getMiddle());
                DoubleFunction<Number> convertProperty = valueType == ValueType.DOUBLE
                    ? property -> property
                    : property -> (long) property;

                var relationshipType = relTypeAndPropertyKeyAndValues.getLeft().name();
                var propertyName = usesPropertyNameColumn ? relTypeAndPropertyKeyAndValues.getMiddle() : null;

                Graph graph = relTypeAndPropertyKeyAndValues.getRight();
                var originalSourceId = graph.toOriginalNodeId(nodeId);
                return graph
                    .streamRelationships(nodeId, Double.NaN)
                    .map(relationshipCursor -> {
                        var originalTargetId = graph.toOriginalNodeId(relationshipCursor.targetId());
                        Number propertyValue = convertProperty.apply(relationshipCursor.property());
                        return outputMarshaller.produce(
                            originalSourceId,
                            originalTargetId,
                            relationshipType,
                            propertyName,
                            propertyValue
                        );
                    });
            })).flatMap(Function.identity());
    }
}
