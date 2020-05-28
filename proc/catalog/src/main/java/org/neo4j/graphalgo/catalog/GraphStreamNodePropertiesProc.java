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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphExportNodePropertiesConfig;
import org.neo4j.graphalgo.config.GraphStreamNodePropertiesConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.NumberType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphStreamNodePropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.streamNodeProperties", mode = READ)
    @Description("Streams the given node properties.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProperties") List<String> nodeProperties,
        @Name(value = "nodeLabels", defaultValue = "['*']") List<String> nodeLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphStreamNodePropertiesConfig config = GraphStreamNodePropertiesConfig.of(
            getUsername(),
            graphName,
            nodeProperties,
            nodeLabels,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), graphName).graphStore();
        config.validate(graphStore);

       return streamNodeProperties(graphStore, config);
    }

    private Stream<Result> streamNodeProperties(GraphStore graphStore, GraphExportNodePropertiesConfig config) {
        Collection<NodeLabel> validNodeLabels = config.validNodeLabels(graphStore);

        var subGraph = graphStore.getGraph(validNodeLabels, graphStore.relationshipTypes(), Optional.empty());
        var nodePropertyKeysAndValues = config.nodeProperties().stream().map(propertyKey -> Pair.of(propertyKey, subGraph.nodeProperties(propertyKey))).collect(Collectors.toList());

        return LongStream
            .range(0, subGraph.nodeCount())
            .boxed()
            .flatMap(nodeId -> {
                var originalId = subGraph.toOriginalNodeId(nodeId);
                return nodePropertyKeysAndValues.stream().map(propertyKeyAndValues -> {
                    NodeLabel label = subGraph.nodeLabels(nodeId).iterator().next();
                    NumberType valueType = graphStore.nodePropertyType(label, propertyKeyAndValues.getKey());

                    double doubleValue = propertyKeyAndValues.getValue().nodeProperty(nodeId);
                    Number numberValue;
                    if (valueType == NumberType.FLOATING_POINT) {
                        numberValue = doubleValue;
                    }
                    else {
                        numberValue = (long) doubleValue;
                    }

                    return new Result(
                        originalId,
                        propertyKeyAndValues.getKey(),
                        numberValue
                    );
                });
            });
    }

    public static class Result {
        public final long nodeId;
        public final String nodeProperty;
        public final Number propertyValue;

        Result(long nodeId, String nodeProperty, Number propertyValue) {
            this.nodeId = nodeId;
            this.nodeProperty = nodeProperty;
            this.propertyValue = propertyValue;
        }
    }

}
