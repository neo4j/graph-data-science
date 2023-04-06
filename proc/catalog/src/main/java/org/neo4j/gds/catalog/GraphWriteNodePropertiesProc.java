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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphWriteNodePropertiesConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteNodePropertiesProc extends CatalogProc {

    @Context
    public NodePropertyExporterBuilder nodePropertyExporterBuilder;

    @Procedure(name = "gds.graph.nodeProperties.write", mode = WRITE)
    @Description("Writes the given node properties to an online Neo4j database.")
    public Stream<Result> writeNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return writeNodeProperties(
            graphName,
            nodeProperties,
            nodeLabels,
            configuration,
            Optional.empty()
        );
    }

    @Procedure(name = "gds.graph.writeNodeProperties", mode = WRITE, deprecatedBy = "gds.graph.nodeProperties.write")
    @Description("Writes the given node properties to an online Neo4j database.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") Object nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") Object nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.nodeProperties.write`";
        return writeNodeProperties(
            graphName,
            nodeProperties,
            nodeLabels,
            configuration,
            Optional.of(deprecationWarning)
        );
    }

    private Stream<Result> writeNodeProperties(
        String graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> configuration,
        Optional<String> deprecationWarning
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphWriteNodePropertiesConfig config = GraphWriteNodePropertiesConfig.of(
            graphName,
            nodeProperties,
            nodeLabels,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        var validNodeLabels = config.validNodeLabels(graphStore);
        var task = Tasks.iterativeFixed(
            "Graph :: NodeProperties :: Write",
            () -> List.of(
                NodePropertyExporter.innerTask("Label", Task.UNKNOWN_VOLUME)
            ),
            validNodeLabels.size()
        );
        var progressTracker = new TaskProgressTracker(
            task,
            executionContext().log(),
            config.writeConcurrency(),
            new JobId(),
            executionContext().taskRegistryFactory(),
            executionContext().userLogRegistryFactory()
        );

        deprecationWarning.ifPresent(progressTracker::logWarning);

        var allNodeProperties = config
            .nodeProperties()
            .stream()
            .map(v -> v.writeProperty())
            .collect(
                Collectors.toList());
        // writing
        Result.Builder builder = new Result.Builder(graphName, allNodeProperties);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
            long propertiesWritten = runWithExceptionLogging(
                "Node property writing failed",
                () -> writeNodeProperties(graphStore, config, validNodeLabels, progressTracker)
            );
            builder.withPropertiesWritten(propertiesWritten);
        }
        // result
        return Stream.of(builder.build());
    }

    private long writeNodeProperties(
        GraphStore graphStore,
        GraphWriteNodePropertiesConfig config,
        Iterable<NodeLabel> validNodeLabels,
        ProgressTracker progressTracker
    ) {
        var propertiesWritten = 0L;

        progressTracker.beginSubTask();
        try {
            for (var label : validNodeLabels) {
                var subGraph = graphStore.getGraph(
                    Collections.singletonList(label),
                    graphStore.relationshipTypes(),
                    Optional.empty()
                );
                var exporter = nodePropertyExporterBuilder
                    .withIdMap(subGraph)
                    .withTerminationFlag(TerminationFlag.wrap(executionContext().terminationMonitor()))
                    .parallel(Pools.DEFAULT, config.writeConcurrency())
                    .withProgressTracker(progressTracker)
                    .withArrowConnectionInfo(config.arrowConnectionInfo())
                    .build();

                var writeNodeProperties =
                    config.nodeProperties().stream()
                        .map(nodePropertyKey ->
                            ImmutableNodeProperty.of(
                                nodePropertyKey.writeProperty(),
                                subGraph.nodeProperties(nodePropertyKey.nodeProperty())
                            )
                        )
                        .collect(Collectors.toList());

                exporter.write(writeNodeProperties);

                propertiesWritten += exporter.propertiesWritten();
            }
        } finally {
            progressTracker.endSubTask();
        }

        return propertiesWritten;
    }

    @SuppressWarnings("unused")
    public static class Result {
        public final long writeMillis;
        public final String graphName;
        public final List<String> nodeProperties;
        public final long propertiesWritten;

        Result(long writeMillis, String graphName, List<String> nodeProperties, long propertiesWritten) {
            this.writeMillis = writeMillis;
            this.graphName = graphName;
            this.nodeProperties = nodeProperties.stream().sorted().collect(Collectors.toList());
            this.propertiesWritten = propertiesWritten;
        }

        static class Builder {
            private final String graphName;
            private final List<String> nodeProperties;
            private long propertiesWritten;
            private long writeMillis;

            Builder(String graphName, List<String> nodeProperties) {
                this.graphName = graphName;
                this.nodeProperties = nodeProperties;
            }

            Builder withWriteMillis(long writeMillis) {
                this.writeMillis = writeMillis;
                return this;
            }

            Builder withPropertiesWritten(long propertiesWritten) {
                this.propertiesWritten = propertiesWritten;
                return this;
            }

            Result build() {
                return new Result(writeMillis, graphName, nodeProperties, propertiesWritten);
            }
        }
    }

}
