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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class StreamNodePropertiesApplication {
    private final Log log;

    public StreamNodePropertiesApplication(Log log) {this.log = log;}

    /*
     * Handy-ish interface for caller, and we just do some intrinsic parameter extraction,
     * then call again to a more specific method. Complexity hiding.
     */
    <T> Stream<T> compute(
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphStore graphStore,
        GraphExportNodePropertiesConfig configuration,
        boolean usesPropertyNameColumn,
        Optional<String> deprecationWarning,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        var nodeLabels = configuration.validNodeLabels(graphStore);

        var subGraph = graphStore.getGraph(nodeLabels, graphStore.relationshipTypes(), Optional.empty());

        var nodePropertyKeysAndValues = configuration.nodeProperties().stream()
            .map(propertyKey -> Pair.of(propertyKey, subGraph.nodeProperties(propertyKey)))
            .collect(Collectors.toList());

        return _compute(
            taskRegistryFactory,
            userLogRegistryFactory,
            configuration,
            subGraph,
            nodePropertyKeysAndValues,
            usesPropertyNameColumn,
            deprecationWarning,
            outputMarshaller
        );
    }

    private <T> Stream<T> _compute(
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphExportNodePropertiesConfig configuration,
        IdMap idMap,
        Collection<Pair<String, NodePropertyValues>> nodePropertyKeysAndValues,
        boolean usesPropertyNameColumn,
        Optional<String> deprecationWarning,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        var task = Tasks.leaf(
            "Graph :: NodeProperties :: Stream",
            idMap.nodeCount() * nodePropertyKeysAndValues.size()
        );

        var progressTracker = new TaskProgressTracker(
            task,
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            configuration.concurrency(),
            new JobId(),
            taskRegistryFactory,
            userLogRegistryFactory
        );

        return computeWithProgressTracking(
            configuration,
            idMap,
            nodePropertyKeysAndValues,
            usesPropertyNameColumn,
            deprecationWarning,
            progressTracker,
            outputMarshaller
        );
    }

    <T> Stream<T> computeWithProgressTracking(
        GraphExportNodePropertiesConfig configuration,
        IdMap idMap,
        Collection<Pair<String, NodePropertyValues>> nodePropertyKeysAndValues,
        boolean usesPropertyNameColumn,
        Optional<String> deprecationWarning,
        ProgressTracker progressTracker,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        // matches the onclose later - we keep this pair in same scope
        progressTracker.beginSubTask();

        deprecationWarning.ifPresent(progressTracker::logWarning);

        return computeNodePropertyStream(
            configuration,
            idMap,
            nodePropertyKeysAndValues,
            usesPropertyNameColumn,
            progressTracker,
            outputMarshaller
        ).onClose(progressTracker::endSubTask);
    }

    <T> Stream<T> computeNodePropertyStream(
        GraphExportNodePropertiesConfig configuration,
        IdMap idMap,
        Collection<Pair<String, NodePropertyValues>> nodePropertyKeysAndValues,
        boolean usesPropertyNameColumn,
        ProgressTracker progressTracker,
        GraphStreamNodePropertyOrPropertiesResultProducer<T> outputMarshaller
    ) {
        Function<Long, List<String>> nodeLabelsFn = configuration.listNodeLabels()
            ? nodeId -> idMap.nodeLabels(nodeId).stream().map(NodeLabel::name).collect(Collectors.toList())
            : nodeId -> Collections.emptyList();

        return LongStream.range(0, idMap.nodeCount())
            .boxed()
            .flatMap(nodeId -> {
                    var originalId = idMap.toOriginalNodeId(nodeId);

                    return nodePropertyKeysAndValues.stream().map(propertyKeyAndValues -> {
                            progressTracker.logProgress();

                            return outputMarshaller.produce(
                                originalId,
                                usesPropertyNameColumn ? propertyKeyAndValues.getKey() : null,
                                propertyKeyAndValues.getValue().getObject(nodeId),
                                nodeLabelsFn.apply(nodeId)
                            );
                        }
                    );
                }
            );
    }
}
