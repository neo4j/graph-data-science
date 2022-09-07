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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphRemoveNodePropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.removeNodeProperties", mode = READ)
    @Description("Removes node properties from a projected graph.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") @NotNull Object nodeProperties,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
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
        // removing
        long propertiesRemoved = runWithExceptionLogging(
            "Node property removal failed",
            () -> removeNodeProperties(graphStore, config)
        );
        // result
        return Stream.of(new Result(graphName, config.nodeProperties(), propertiesRemoved));
    }

    @NotNull
    private Long removeNodeProperties(GraphStore graphStore, org.neo4j.gds.catalog.GraphRemoveNodePropertiesConfig config) {
        var removedPropertiesCount = new MutableLong(0);

        config.nodeProperties().forEach(propertyKey -> {
            removedPropertiesCount.add(graphStore.nodeProperty(propertyKey).values().size());
            graphStore.removeNodeProperty(propertyKey);
        });

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
