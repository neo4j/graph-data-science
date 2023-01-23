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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphAccessGraphPropertiesConfig;
import org.neo4j.gds.config.GraphStreamGraphPropertiesConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphStreamGraphPropertiesProc extends CatalogProc {

    @Procedure(name = "gds.alpha.graph.graphProperty.stream", mode = READ)
    @Description("Streams the given graph property.")
    public Stream<PropertyResult> streamProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "graphProperty") String graphProperty,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        // input
        var cypherConfig = CypherMapWrapper.create(configuration);
        var config = GraphStreamGraphPropertiesConfig.of(
            graphName,
            graphProperty,
            cypherConfig
        );

        // validation
        validateConfig(cypherConfig, config);
        var graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        return streamGraphProperties(graphStore, config);
    }

    private Stream<PropertyResult> streamGraphProperties(
        GraphStore graphStore,
        GraphAccessGraphPropertiesConfig config
    ) {
        return graphStore
            .graphPropertyValues(config.graphProperty())
            .objects()
            .map(PropertyResult::new);
    }

    @SuppressWarnings("unused")
    public static class PropertyResult {
        public final Object propertyValue;

        PropertyResult(Object propertyValue) {
            this.propertyValue = propertyValue;
        }
    }
}
