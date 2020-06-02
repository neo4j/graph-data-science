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
package org.neo4j.graphalgo.gdl;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.HugeGraphUtil;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.NodePropertiesBuilder;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class GDLFactory extends GraphStoreFactory<GraphCreateFromGDLConfig> {

    private static final int NO_SUCH_PROPERTY = -2;
    private final GDLHandler gdlHandler;

    public static GDLFactory of(String gdlGraph) {
        return of(AuthSubject.ANONYMOUS.username(), "graph", gdlGraph);
    }

    public static GDLFactory of(String username, String graphName, String gdlGraph) {
        return of(ImmutableGraphCreateFromGDLConfig.builder()
            .username(username)
            .graphName(graphName)
            .gdlGraph(gdlGraph)
            .build());
    }

    public static GDLFactory of(GraphCreateFromGDLConfig config) {
        var gdlHandler = new GDLHandler.Builder()
            .setDefaultVertexLabel(NodeLabel.ALL_NODES.name)
            .setDefaultEdgeLabel(RelationshipType.ALL_RELATIONSHIPS.name)
            .buildFromString(config.gdlGraph());

        var graphDimensions = GraphDimensionsGdlReader.of(gdlHandler);

        return new GDLFactory(gdlHandler, config, graphDimensions);
    }

    private GDLFactory(
        GDLHandler gdlHandler,
        GraphCreateFromGDLConfig graphCreateConfig,
        GraphDimensions graphDimensions
    ) {
        super(graphCreateConfig, NO_API_CONTEXT, graphDimensions);
        this.gdlHandler = gdlHandler;
    }

    public long nodeId(String variable) {
        return gdlHandler.getVertexCache().get(variable).getId();
    }

    @Override
    public ImportResult build() {
        var nodes = loadNodes();
        var relationships = loadRelationships(nodes.idMap());
        var graphStore = CSRGraphStore.of(nodes.idMap(), nodes.properties(), relationships, Map.of(), 1, loadingContext.tracker());
        return ImportResult.of(dimensions, graphStore);
    }

    private IdsAndProperties loadNodes() {
        var idMapBuilder = HugeGraphUtil.idMapBuilder(
            dimensions.highestNeoId(),
            loadingContext.executor(),
            loadingContext.tracker()
        );

        gdlHandler.getVertices().forEach(vertex -> idMapBuilder.addNode(
            vertex.getId(),
            vertex.getLabels().stream().map(NodeLabel::of).toArray(NodeLabel[]::new)
        ));

        var idMap = idMapBuilder.build();

        return IdsAndProperties.of(idMap, loadNodeProperties(idMap));
    }

    private Map<NodeLabel, Map<PropertyMapping, NodeProperties>> loadNodeProperties(IdMapping idMap) {
        var propertyKeysByLabel = new HashMap<NodeLabel, Set<PropertyMapping>>();
        var propertyBuilders = new HashMap<PropertyMapping, NodePropertiesBuilder>();

        gdlHandler.getVertices().forEach(vertex -> {
            vertex.getProperties().forEach((propertyKey, propertyValue) ->
                propertyBuilders.computeIfAbsent(PropertyMapping.of(propertyKey), (key) -> {
                    vertex.getLabels().stream()
                        .map(NodeLabel::of)
                        .forEach(nodeLabel -> propertyKeysByLabel
                            .computeIfAbsent(nodeLabel, (ignore) -> new HashSet<>())
                            .add(key)
                        );
                    return NodePropertiesBuilder.of(
                        dimensions.nodeCount(),
                        loadingContext.tracker(),
                        PropertyMapping.DEFAULT_FALLBACK_VALUE,
                        NO_SUCH_PROPERTY,
                        key.propertyKey(),
                        1
                    );
                }).set(idMap.toMappedNodeId(vertex.getId()), gdsValue(vertex, propertyKey, propertyValue)));
        });

        var nodeProperties = propertyBuilders
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build()));

        return propertyKeysByLabel.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream().collect(Collectors.toMap(
                    propertyKey -> propertyKey,
                    nodeProperties::get
                ))
            ));
    }

    private double gdsValue(Vertex vertex, String propertyKey, Object gdlValue) {
        if (gdlValue instanceof Number) {
            return ((Number) gdlValue).doubleValue();
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Node property '%s' of must be of type Number, but was %s for %s.",
                propertyKey,
                gdlValue.getClass(),
                vertex
            ));
        }
    }

    private Map<RelationshipType, HugeGraph.TopologyCSR> loadRelationships(IdMap nodes) {
        var relTypeImporters = gdlHandler.getEdges().stream()
            .map(Element::getLabel)
            .distinct()
            .collect(Collectors.toMap(
                relType -> relType,
                relType -> HugeGraphUtil.createRelImporter(
                    nodes,
                    graphCreateConfig.orientation(),
                    false,
                    Aggregation.NONE,
                    loadingContext.executor(),
                    loadingContext.tracker()
                )
            ));

        gdlHandler
            .getEdges()
            .forEach(edge -> relTypeImporters
                .get(edge.getLabel())
                .add(edge.getSourceVertexId(), edge.getTargetVertexId()));

        return relTypeImporters.entrySet().stream().collect(Collectors.toMap(
            entry -> RelationshipType.of(entry.getKey()),
            entry -> entry.getValue().build().topology()
        ));
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.empty();
    }

    @Override
    protected ProgressLogger initProgressLogger() {
        return ProgressLogger.NULL_LOGGER;
    }

    private static final GraphLoaderContext NO_API_CONTEXT = new GraphLoaderContext() {
        @Override
        public GraphDatabaseAPI api() {
            return null;
        }

        @Override
        public Log log() {
            return NullLog.getInstance();
        }
    };

    private static final class GraphDimensionsGdlReader {

        static GraphDimensions of(GDLHandler gdlHandler) {
            var nodeCount = gdlHandler.getVertices().size();
            var relCount = gdlHandler.getEdges().size();

            return ImmutableGraphDimensions.builder()
                .nodeCount(nodeCount)
                .maxRelCount(relCount)
                .build();
        }
    }
}
