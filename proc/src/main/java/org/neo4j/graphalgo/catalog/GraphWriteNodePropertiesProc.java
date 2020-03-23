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
import org.neo4j.graphalgo.config.GraphWriteNodePropertiesConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.ImmutableNodeProperty;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteNodePropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.writeNodeProperties", mode = WRITE)
    @Description("Writes the given node properties to an online Neo4j database.")
    public Stream<Result> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") List<String> nodeProperties,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphWriteNodePropertiesConfig config = GraphWriteNodePropertiesConfig.of(
            getUsername(),
            graphName,
            nodeProperties,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), graphName).graphStore();
        config.validate(graphStore);
        // writing
        long propertiesWritten = runWithExceptionLogging(
            "Graph creation failed",
            () -> writeNodeProperties(graphStore, nodeProperties, config.writeConcurrency())
        );
        // result
        return Stream.of(new Result(graphName, nodeProperties, propertiesWritten));
    }

    private long writeNodeProperties(GraphStore graphStore, List<String> nodePropertyKeys, int writeConcurrency) {
        NodePropertyExporter exporter = NodePropertyExporter
            .of(api, graphStore.nodes(), TerminationFlag.wrap(transaction))
            .parallel(Pools.DEFAULT, writeConcurrency)
            .withLog(log)
            .build();

        Collection<NodePropertyExporter.NodeProperty<?>> nodeProperties = nodePropertyKeys.stream()
            .map(nodePropertyKey -> ImmutableNodeProperty.of(
                nodePropertyKey,
                graphStore.nodeProperty(nodePropertyKey),
                (PropertyTranslator.OfDouble<NodeProperties>) NodeProperties::nodeProperty
            )).collect(Collectors.toList());

        exporter.write(nodeProperties);

        return nodePropertyKeys.stream().mapToLong(nodePropertyKey -> graphStore.nodeProperty(nodePropertyKey).size()).sum();
    }

    public static class Result {
        public final String graphName;

        public final List<String> nodeProperties;

        public final long propertiesWritten;

        Result(String graphName, List<String> nodeProperties, long propertiesWritten) {
            this.graphName = graphName;
            this.nodeProperties = nodeProperties.stream().sorted().collect(Collectors.toList());
            this.propertiesWritten = propertiesWritten;
        }
    }

}
