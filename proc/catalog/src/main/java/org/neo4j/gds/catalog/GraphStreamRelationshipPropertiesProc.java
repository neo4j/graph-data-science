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
package org.neo4j.gds.catalog;

import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.config.GraphStreamRelationshipPropertiesConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphStreamRelationshipPropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.relationshipProperties.stream", mode = READ)
    @Description("Streams the given relationship properties.")
    public Stream<PropertiesResult> streamRelationshipProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") List<String> relationshipProperties,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return streamRelationshipProperties(
            graphName,
            configuration,
            relationshipProperties,
            relationshipTypes,
            PropertiesResult::new,
            Optional.empty()
        );
    }

    @Procedure(name = "gds.graph.streamRelationshipProperties", mode = READ, deprecatedBy = "gds.graph.relationshipProperties.stream")
    @Description("Streams the given relationship properties.")
    public Stream<PropertiesResult> streamProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") List<String> relationshipProperties,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.relationshipProperties.stream`";
        return streamRelationshipProperties(graphName, configuration, relationshipProperties, relationshipTypes, PropertiesResult::new, Optional.of(deprecationWarning));
    }

    @Procedure(name = "gds.graph.relationshipProperty.stream", mode = READ)
    @Description("Streams the given relationship property.")
    public Stream<PropertyResult> streamRelationshipProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperty") String relationshipProperty,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return streamRelationshipProperties(
            graphName,
            configuration,
            List.of(relationshipProperty),
            relationshipTypes,
            (sourceId, targetId, relationshipType, propertyName, propertyValue) -> new PropertyResult(
                sourceId,
                targetId,
                relationshipType,
                propertyValue
            ),
            Optional.empty()
        );
    }

    @Procedure(name = "gds.graph.streamRelationshipProperty", mode = READ, deprecatedBy = "gds.graph.relationshipProperty.stream")
    @Description("Streams the given relationship property.")
    public Stream<PropertyResult> streamProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") String relationshipProperty,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.relationshipProperty.stream`";
        return streamRelationshipProperties(
            graphName,
            configuration,
            List.of(relationshipProperty),
            relationshipTypes,
            (sourceId, targetId, relationshipType, propertyName, propertyValue) -> new PropertyResult(
                sourceId,
                targetId,
                relationshipType,
                propertyValue
            ),
            Optional.of(deprecationWarning)
        );
    }

    private <R> Stream<R> streamRelationshipProperties(
        String graphName,
        Map<String, Object> configuration,
        List<String> relationshipProperties,
        List<String> relationshipTypes,
        ResultProducer<R> producer,
        Optional<String> deprecationWarning
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphStreamRelationshipPropertiesConfig config = GraphStreamRelationshipPropertiesConfig.of(
            graphName,
            relationshipProperties,
            relationshipTypes,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        Collection<RelationshipType> validRelationshipTypes = config.validRelationshipTypes(graphStore);

        var relationshipPropertyKeysAndValues = validRelationshipTypes
            .stream()
            .flatMap(relType -> config.relationshipProperties()
                .stream()
                .filter(propertyKey -> graphStore.hasRelationshipProperty(relType, propertyKey))
                .map(propertyKey -> Triple.of(relType, propertyKey, graphStore.getGraph(relType, Optional.of(propertyKey))))
            )
            .collect(Collectors.toList());
        var usesPropertyNameColumn = executionContext().callContext().outputFields().anyMatch(field -> field.equals("relationshipProperty"));

        var task = Tasks.leaf(
            "Graph :: RelationshipProperties :: Stream",
            graphStore.nodeCount() * relationshipPropertyKeysAndValues.size()
        );

        var taskProgressTracker = new TaskProgressTracker(
            task,
            log,
            config.concurrency(),
            new JobId(),
            executionContext().taskRegistryFactory(),
            executionContext().userLogRegistryFactory()
        );
        taskProgressTracker.beginSubTask();

        deprecationWarning.ifPresent(taskProgressTracker::logWarning);

        var resultStream = LongStream
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
                        return producer.produce(
                            originalSourceId,
                            originalTargetId,
                            relationshipType,
                            propertyName,
                            propertyValue
                        );
                    });
            })).flatMap(Function.identity())
            .onClose(taskProgressTracker::endSubTask);

        try (var statement = transaction.acquireStatement()) {
            statement.registerCloseableResource(resultStream);
            return resultStream;
        }
    }

    @SuppressWarnings("unused")
    public static class PropertiesResult {
        public final long sourceNodeId;
        public final long targetNodeId;
        public final String relationshipType;
        public final String relationshipProperty;
        public final Number propertyValue;

        PropertiesResult(
            long sourceNodeId,
            long targetNodeId,
            String relationshipType,
            String relationshipProperty,
            Number propertyValue
        ) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.relationshipType = relationshipType;
            this.relationshipProperty = relationshipProperty;
            this.propertyValue = propertyValue;
        }
    }

    @SuppressWarnings("unused")
    public static class PropertyResult {
        public final long sourceNodeId;
        public final long targetNodeId;
        public final String relationshipType;
        public final Number propertyValue;

        PropertyResult(long sourceNodeId, long targetNodeId, String relationshipType, Number propertyValue) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.relationshipType = relationshipType;
            this.propertyValue = propertyValue;
        }
    }
    interface ResultProducer<R> {
        R produce(long sourceId, long targetId, String relationshipType, @Nullable String propertyName, Number propertyValue);
    }

}
