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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphStreamNodePropertiesConfig;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphStreamNodePropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.nodeProperties.stream", mode = READ)
    @Description("Streams the given node properties.")
    public Stream<PropertiesResult> streamNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return streamNodeProperties(
            graphName,
            configuration,
            nodeProperties,
            nodeLabels,
            PropertiesResult::new
        );
    }

    @Procedure(name = "gds.graph.streamNodeProperties", mode = READ, deprecatedBy = "gds.graph.nodeProperties.stream")
    @Description("Streams the given node properties.")
    public Stream<PropertiesResult> streamProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.nodeProperties.stream`";
        return streamNodeProperties(
            graphName,
            configuration,
            nodeProperties,
            nodeLabels,
            PropertiesResult::new,
            Optional.of(deprecationWarning)
        );
    }

    @Procedure(name = "gds.graph.nodeProperty.stream", mode = READ)
    @Description("Streams the given node property.")
    public Stream<PropertyResult> streamNodeProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") String nodeProperty,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return streamNodeProperties(
            graphName,
            configuration,
            List.of(nodeProperty),
            nodeLabels,
            (nodeId, propertyName, propertyValue) -> new PropertyResult(nodeId, propertyValue)
        );
    }

    @Procedure(name = "gds.graph.streamNodeProperty", mode = READ, deprecatedBy = "gds.graph.nodeProperty.stream")
    @Description("Streams the given node property.")
    public Stream<PropertyResult> streamProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") String nodeProperty,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.nodeProperty.stream`";
        return streamNodeProperties(
            graphName,
            configuration,
            List.of(nodeProperty),
            nodeLabels,
            (nodeId, propertyName, propertyValue) -> new PropertyResult(nodeId, propertyValue),
            Optional.of(deprecationWarning)
        );
    }

    private <R> Stream<R> streamNodeProperties(
        String graphName,
        Map<String, Object> configuration,
        Object nodeProperties,
        Object nodeLabels,
        ResultProducer<R> producer
    ) {
        return streamNodeProperties(graphName, configuration, nodeProperties, nodeLabels, producer,Optional.empty());
    }

    private <R> Stream<R> streamNodeProperties(
        String graphName,
        Map<String, Object> configuration,
        Object nodeProperties,
        Object nodeLabels,
        ResultProducer<R> producer,
        Optional<String> deprecationWarning
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphStreamNodePropertiesConfig config = GraphStreamNodePropertiesConfig.of(
            graphName,
            nodeProperties,
            nodeLabels,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        Collection<NodeLabel> validNodeLabels = config.validNodeLabels(graphStore);

        var subGraph = graphStore.getGraph(validNodeLabels, graphStore.relationshipTypes(), Optional.empty());
        var nodePropertyKeysAndValues = config.nodeProperties().stream().map(propertyKey -> Pair.of(propertyKey, subGraph.nodeProperties(propertyKey))).collect(Collectors.toList());
        var usesPropertyNameColumn = executionContext().returnColumns().contains("nodeProperty");

        var task = Tasks.leaf(
            "Graph :: NodeProperties :: Stream",
            subGraph.nodeCount() * nodePropertyKeysAndValues.size()
        );

        var taskProgressTracker = new TaskProgressTracker(
            task,
            executionContext().log(),
            config.concurrency(),
            new JobId(),
            executionContext().taskRegistryFactory(),
            executionContext().userLogRegistryFactory()
        );
        taskProgressTracker.beginSubTask();

        deprecationWarning.ifPresent(taskProgressTracker::logWarning);

        var resultStream = LongStream
            .range(0, subGraph.nodeCount())
            .boxed()
            .flatMap(nodeId -> {
                var originalId = subGraph.toOriginalNodeId(nodeId);

                return nodePropertyKeysAndValues.stream().map(propertyKeyAndValues -> {
                    taskProgressTracker.logProgress();
                    return producer.produce(
                        originalId,
                        usesPropertyNameColumn ? propertyKeyAndValues.getKey() : null,
                        propertyKeyAndValues.getValue().getObject(nodeId)
                    );
                });
            })
            .onClose(taskProgressTracker::endSubTask);

        try (var statement = transaction.acquireStatement()) {
            statement.registerCloseableResource(resultStream);
            return resultStream;
        }
    }

    @SuppressWarnings("unused")
    public static class PropertiesResult {
        public final long nodeId;
        public final String nodeProperty;
        public final Object propertyValue;

        PropertiesResult(long nodeId, String nodeProperty, Object propertyValue) {
            this.nodeId = nodeId;
            this.nodeProperty = nodeProperty;
            this.propertyValue = propertyValue;
        }
    }

    @SuppressWarnings("unused")
    public static class PropertyResult {
        public final long nodeId;
        public final Object propertyValue;

        PropertyResult(long nodeId, Object propertyValue) {
            this.nodeId = nodeId;
            this.propertyValue = propertyValue;
        }
    }

    interface ResultProducer<R> {
        R produce(long nodeId, String propertyName, Object propertyValue);
    }

}
