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

import org.neo4j.gds.ProcPreconditions;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphRemoveGraphPropertiesConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphDropGraphPropertiesProc extends CatalogProc {

    @Procedure(name = "gds.alpha.graph.graphProperty.drop", mode = READ)
    @Description("Removes a graph property from a projected graph.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "graphProperty") String graphProperty,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        var config = GraphRemoveGraphPropertiesConfig.of(
            graphName,
            graphProperty,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        // removing
        long propertiesRemoved = graphStore.graphPropertyValues(graphProperty).valuesStored();
        runWithExceptionLogging(
            "Graph property removal failed",
            () -> graphStore.removeGraphProperty(graphProperty)
        );

        // result
        return Stream.of(new Result(graphName, graphProperty, propertiesRemoved));
    }

    @SuppressWarnings("unused")
    public static class Result {
        public final String graphName;
        public final String graphProperty;
        public final long propertiesRemoved;

        Result(String graphName, String graphProperty, long propertiesRemoved) {
            this.graphName = graphName;
            this.graphProperty = graphProperty;
            this.propertiesRemoved = propertiesRemoved;
        }
    }

}
