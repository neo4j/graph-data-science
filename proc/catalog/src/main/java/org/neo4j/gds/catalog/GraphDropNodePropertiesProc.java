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

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.ProcPreconditions;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphRemoveNodePropertiesConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphDropNodePropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.nodeProperties.drop", mode = READ)
    @Description("Removes node properties from a projected graph.")
    public Stream<Result> dropNodeProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") List<String> nodeProperties,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return dropNodeProperties(graphName, nodeProperties, configuration, Optional.empty());
    }

    @Procedure(name = "gds.graph.removeNodeProperties", mode = READ, deprecatedBy = "gds.graph.nodeProperties.drop")
    @Description("Removes node properties from a projected graph.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") List<String> nodeProperties,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.nodeProperties.drop`";
        return dropNodeProperties(graphName, nodeProperties, configuration, Optional.of(deprecationWarning));
    }

    private Stream<Result> dropNodeProperties(
        String graphName,
        List<String> nodeProperties,
        Map<String, Object> configuration,
        Optional<String> deprecationWarning
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphRemoveNodePropertiesConfig config = GraphRemoveNodePropertiesConfig.of(
            graphName,
            nodeProperties,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        // progress tracking
        var task = Tasks.leaf("Graph :: Node properties :: Drop", config.nodeProperties().size());
        var progressTracker = new TaskProgressTracker(
            task,
            log,
            1,
            new JobId(),
            taskRegistryFactory,
            userLogRegistryFactory
        );

        deprecationWarning.ifPresent(progressTracker::logWarning);

        // removing
        long propertiesRemoved = runWithExceptionLogging(
            "Node property removal failed",
            () -> dropNodeProperties(graphStore, config, progressTracker)
        );
        // result
        return Stream.of(new Result(graphName, nodeProperties, propertiesRemoved));
    }

    @NotNull
    private Long dropNodeProperties(
        GraphStore graphStore,
        GraphRemoveNodePropertiesConfig config,
        TaskProgressTracker progressTracker
    ) {
        var removedPropertiesCount = new MutableLong(0);

        progressTracker.beginSubTask();
        config.nodeProperties().forEach(propertyKey -> {
            removedPropertiesCount.add(graphStore.nodeProperty(propertyKey).values().size());
            graphStore.removeNodeProperty(propertyKey);
            progressTracker.logProgress();
        });

        progressTracker.endSubTask();
        return removedPropertiesCount.longValue();
    }

    @SuppressWarnings("unused")
    public static class Result {
        public final String graphName;
        public final List<String> nodeProperties;
        public final long propertiesRemoved;

        Result(String graphName, List<String> nodeProperties, long propertiesRemoved) {
            this.graphName = graphName;
            this.nodeProperties = nodeProperties.stream().sorted().collect(Collectors.toList());
            this.propertiesRemoved = propertiesRemoved;
        }
    }

}
