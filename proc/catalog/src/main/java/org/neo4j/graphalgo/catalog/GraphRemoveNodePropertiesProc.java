/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.catalog;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphRemoveNodePropertiesConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class GraphRemoveNodePropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.removeNodeProperties", mode = WRITE)
    @Description("Removes node properties from an in-memory graph.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") List<String> nodeProperties,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphRemoveNodePropertiesConfig config = GraphRemoveNodePropertiesConfig.of(
            getUsername(),
            graphName,
            nodeProperties,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), graphName).graphStore();
        config.validate(graphStore);
        // removing
        long propertiesRemoved = runWithExceptionLogging(
            "Node property removal failed",
            () -> {
                long sum = config.nodeProperties().stream()
                    .map(graphStore::nodeProperty)
                    .mapToLong(NodeProperties::size)
                    .sum();
                config.nodeProperties().forEach(graphStore::removeNodeProperty);
                return sum;
            }
        );
        // result
        return Stream.of(new Result(graphName, nodeProperties, propertiesRemoved));
    }

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
