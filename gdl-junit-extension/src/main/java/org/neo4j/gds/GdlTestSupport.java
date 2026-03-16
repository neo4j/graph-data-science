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
package org.neo4j.gds;

import org.apache.commons.lang3.tuple.Pair;
import org.immutables.builder.Builder;
import org.neo4j.gds.TestSupportGdlRecords.NodeLabelsAndProperties;
import org.neo4j.gds.TestSupportGdlRecords.RelationshipTypeProperties;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.extension.GdlSupportPerMethodExtension;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.gdl.ImmutableGraphProjectFromGdlConfig;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

import static org.neo4j.gds.NodeLabel.ALL_NODES;

public final class GdlTestSupport {

    private GdlTestSupport() {}

    public static TestGraph fromGdl(String gdl) {
        return new GdlBuilder().gdl(gdl).build();
    }

    public static TestGraph fromGdl(String gdl, long idOffset) {
        return new GdlBuilder().gdl(gdl).idSupplier(new OffsetIdSupplier(idOffset)).build();
    }

    public static TestGraph fromGdl(String gdl, String name) {
        return new GdlBuilder().gdl(gdl).name(name).build();
    }

    public static TestGraph fromGdl(String gdl, Orientation orientation) {
        return new GdlBuilder().gdl(gdl).orientation(orientation).build();
    }

    public static TestGraph fromGdl(String gdl, Orientation orientation, String name) {
        return new GdlBuilder().gdl(gdl).orientation(orientation).name(name).build();
    }

    @Builder.Factory
    public static TestGraph gdl(
        String gdl,
        Optional<String> name,
        Optional<Orientation> orientation,
        Optional<Aggregation> aggregation,
        Optional<LongSupplier> idSupplier,
        Optional<DatabaseId> databaseId,
        Optional<Boolean> indexInverse
    ) {
        Objects.requireNonNull(gdl);

        var graphName = name.orElse("graph");

        var config = ImmutableGraphProjectFromGdlConfig.builder()
            .gdlGraph(gdl)
            .graphName(graphName)
            .orientation(orientation.orElse(Orientation.NATURAL))
            .aggregation(aggregation.orElse(Aggregation.DEFAULT))
            .indexInverse(indexInverse.orElse(false))
            .build();

        var gdlFactory = GdlFactory
            .builder()
            .nodeIdFunction(idSupplier.orElse(new OffsetIdSupplier(0L)))
            .graphProjectConfig(config)
            .databaseId(databaseId.orElse(GdlSupportPerMethodExtension.DATABASE_ID))
            .build();

        return new TestGraph(gdlFactory.build().getUnion(), gdlFactory::nodeId, graphName);
    }

    public static GraphStore graphStoreFromGDL(String gdl) {
        Objects.requireNonNull(gdl);

        return GdlFactory.of(gdl).build();
    }

    public static String gdlFromGraphStore(GraphStore graphStore) {
        Objects.requireNonNull(graphStore);
        StringBuilder sb = new StringBuilder();
        var graph = graphStore.getUnion();

        graph.forEachNode(nodeId -> {
            List<NodeLabel> nodeLabels = graph.nodeLabels(nodeId);

            List<String> labels = nodeLabels.stream()
                .filter(label -> !label.equals(ALL_NODES))
                .map(NodeLabel::name).toList();

            List<Pair<String, NodeProperty>> properties = graphStore.nodePropertyKeys().stream()
                .filter(key -> graphStore.hasNodeProperty(nodeLabels, key))
                .map(key -> Pair.of(key, graphStore.nodeProperty(key))).toList();

            NodeLabelsAndProperties node = new NodeLabelsAndProperties(nodeId, labels, properties);
            sb.append(node.toGdl());
            sb.append(System.lineSeparator());
            return true;
        });

        graphStore.relationshipTypes()
            .forEach(relationshipType -> {
                var relationTypeProperties = RelationshipTypeProperties.of(graphStore, relationshipType);
                relationTypeProperties.toGdl(relationshipType).forEach(relationship -> {
                    sb.append(relationship);
                    sb.append(System.lineSeparator());
                });
            });

        return sb.toString();
    }

    @Builder.Factory
    public static GraphStore gdlGraphStore(
        String gdl,
        Optional<String> name,
        Optional<Orientation> orientation,
        Optional<Aggregation> aggregation,
        Optional<LongSupplier> idSupplier,
        Optional<DatabaseId> databaseId,
        Optional<Boolean> indexInverse
    ) {
        Objects.requireNonNull(gdl);

        var graphName = name.orElse("graph");

        var config = ImmutableGraphProjectFromGdlConfig.builder()
            .gdlGraph(gdl)
            .graphName(graphName)
            .orientation(orientation.orElse(Orientation.NATURAL))
            .aggregation(aggregation.orElse(Aggregation.DEFAULT))
            .indexInverse(indexInverse.orElse(false))
            .build();

        var gdlFactory = GdlFactory
            .builder()
            .nodeIdFunction(idSupplier.orElse(new OffsetIdSupplier(0L)))
            .graphProjectConfig(config)
            .databaseId(databaseId.orElse(GdlSupportPerMethodExtension.DATABASE_ID))
            .build();

        return gdlFactory.build();
    }


}
