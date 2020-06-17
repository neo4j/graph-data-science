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
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphStreamRelationshipPropertiesConfig;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.procedure.Mode.READ;

public class GraphStreamRelationshipPropertiesProc extends CatalogProc {

    @Procedure(name = "gds.graph.streamRelationshipProperties", mode = READ)
    @Description("Streams the given relationship properties.")
    public Stream<PropertiesResult> streamProperties(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") List<String> relationshipProperties,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphStreamRelationshipPropertiesConfig config = GraphStreamRelationshipPropertiesConfig.of(
            getUsername(),
            graphName,
            relationshipProperties,
            relationshipTypes,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), graphName).graphStore();
        config.validate(graphStore);

       return streamRelationshipProperties(graphStore, config, PropertiesResult::new);
    }

    @Procedure(name = "gds.graph.streamRelationshipProperty", mode = READ)
    @Description("Streams the given relationship property.")
    public Stream<PropertyResult> streamProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipProperties") String relationshipProperty,
        @Name(value = "relationshipLabels", defaultValue = "['*']") List<String> relationshipLabels,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphStreamRelationshipPropertiesConfig config = GraphStreamRelationshipPropertiesConfig.of(
            getUsername(),
            graphName,
            List.of(relationshipProperty),
            relationshipLabels,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), graphName).graphStore();
        config.validate(graphStore);

        return streamRelationshipProperties(graphStore, config, (sourceId, targetId, propertyName, propertyValue) -> new PropertyResult(sourceId, targetId, propertyValue));
    }

    private <R> Stream<R> streamRelationshipProperties(GraphStore graphStore, GraphStreamRelationshipPropertiesConfig config, ResultProducer<R> producer) {
        Collection<RelationshipType> validRelationshipTypes = config.validRelationshipTypes(graphStore);

        var relationshipPropertyKeysAndValues = config
            .relationshipProperties()
            .stream()
            .map(propertyKey -> Pair.of(propertyKey, graphStore.getGraph(validRelationshipTypes, Optional.of(propertyKey))))
            .collect(Collectors.toList());
        var usesPropertyNameColumn = callContext.outputFields().anyMatch(field -> field.equals("relationshipProperty"));

        return LongStream
            .range(0, graphStore.nodeCount())
            .mapToObj(nodeId -> relationshipPropertyKeysAndValues.stream().flatMap(propertyKeyAndValues -> {
                NumberType valueType = graphStore.relationshipPropertyType(propertyKeyAndValues.getKey());
                Graph graph = propertyKeyAndValues.getRight();

                var originalSourceId = graph.toOriginalNodeId(nodeId);
                return StreamSupport.stream(graph.streamRelationships(nodeId, Double.NaN), false)
                    .map(relationshipCursor -> {
                        var originalTargetId = graph.toOriginalNodeId(relationshipCursor.targetId());
                        Number numberValue;
                        if (valueType == NumberType.FLOATING_POINT) {
                            numberValue = relationshipCursor.property();
                        }
                        else {
                            numberValue = (long) relationshipCursor.property();
                        }

                        return producer.produce(
                            originalSourceId,
                            originalTargetId,
                            usesPropertyNameColumn ? propertyKeyAndValues.getKey() : null,
                            numberValue
                        );
                    });
            })).flatMap(Function.identity());
    }

    public static class PropertiesResult {
        public final long sourceNodeId;
        public final long targetNodeId;
        public final String relationshipProperty;
        public final Number propertyValue;

        PropertiesResult(long sourceNodeId, long targetNodeId, String relationshipProperty, Number propertyValue) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.relationshipProperty = relationshipProperty;
            this.propertyValue = propertyValue;
        }
    }

    public static class PropertyResult {
        public final long sourceNodeId;
        public final long targetNodeId;
        public final Number propertyValue;

        PropertyResult(long sourceNodeId, long targetNodeId, Number propertyValue) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.propertyValue = propertyValue;
        }
    }
    interface ResultProducer<R> {
        R produce(long sourceId, long targetId, String propertyName, Number propertyValue);
    }

}
