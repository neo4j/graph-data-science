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

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphWriteNodePropertiesConfig;
import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.ImmutableNodeProperty;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteNodePropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.writeNodeProperties", mode = WRITE)
    @Description("Writes the given node properties to an online Neo4j database.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") List<String> nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") List<String> nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphWriteNodePropertiesConfig config = GraphWriteNodePropertiesConfig.of(
            username(),
            graphName,
            nodeProperties,
            nodeLabels,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        // writing
        Result.Builder builder = new Result.Builder(graphName, nodeProperties);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
            long propertiesWritten = runWithExceptionLogging(
                "Node property writing failed",
                () -> writeNodeProperties(graphStore, config)
            );
            builder.withPropertiesWritten(propertiesWritten);
        }
        // result
        return Stream.of(builder.build());
    }

    private long writeNodeProperties(GraphStore graphStore, GraphWriteNodePropertiesConfig config) {
        Collection<NodeLabel> validNodeLabels = config.validNodeLabels(graphStore);

        var propertiesWritten = 0L;
        var labelsWritten = 1L;
        var labelCount = validNodeLabels.size();

        for (var label : validNodeLabels) {
            var subGraph = graphStore.getGraph(
                Collections.singletonList(label),
                graphStore.relationshipTypes(),
                Optional.empty()
            );

            // We track the progress for each label individually.
            // This needs to be reflected in the log output.
            var taskName = formatWithLocale(
                "WriteNodeProperties - Label %d of %d [Label='%s']",
                labelsWritten++,
                labelCount,
                label.name()
            );

            var progressLogger = new BatchingProgressLogger(
                log,
                subGraph.nodeCount(),
                taskName,
                config.writeConcurrency()
            );

            var exporter = NodePropertyExporter
                .builder(TransactionContext.of(api, procedureTransaction), subGraph, TerminationFlag.wrap(transaction))
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .withProgressLogger(progressLogger)
                .build();

            var writeNodeProperties =
                config.nodeProperties().stream()
                    .map(nodePropertyKey ->
                        ImmutableNodeProperty.of(
                            nodePropertyKey,
                            subGraph.nodeProperties(nodePropertyKey)
                        )
                    )
                    .collect(Collectors.toList());

            exporter.write(writeNodeProperties);
            propertiesWritten += exporter.propertiesWritten();
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
