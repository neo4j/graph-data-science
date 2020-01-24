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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.RelationshipProjectionMappings;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.ProjectionParser;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.InternalReadOps;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class GraphDimensionsReader extends StatementFunction<GraphDimensions> {
    private final GraphSetup setup;
    private final boolean readTokens;

    public GraphDimensionsReader(
            GraphDatabaseAPI api,
            GraphSetup setup,
            boolean readTokens) {
        super(api);
        this.setup = setup;
        this.readTokens = readTokens;
    }

    @Override
    public GraphDimensions apply(final KernelTransaction transaction) throws RuntimeException {
        TokenRead tokenRead = transaction.tokenRead();
        Read dataRead = transaction.dataRead();

        NodeLabelIds nodeLabelIds = new NodeLabelIds();
        if (readTokens) {
            ProjectionParser.parse(setup.nodeLabel())
                .stream()
                .map(tokenRead::nodeLabel)
                .forEach(nodeLabelIds.ids::add);
        }

        RelationshipProjectionMappings.Builder mappingsBuilder = new RelationshipProjectionMappings.Builder();
        if (readTokens) {
            if (setup.legacyMode()) {
                if (!loadAnyRelationshipType()) {
                    Set<String> types = ProjectionParser.parse(setup.relationshipType());
                    for (String typeName : types) {
                        int typeId = tokenRead.relationshipType(typeName);
                        RelationshipProjectionMapping typeMapping = RelationshipProjectionMapping.of(typeName, typeId);
                        mappingsBuilder.addMapping(typeMapping);
                    }
                }
            } else {
                for (Map.Entry<ElementIdentifier, RelationshipProjection> entry : setup.relationshipProjections().projections().entrySet()) {
                    RelationshipProjection relationshipProjection = entry.getValue();

                    if (!relationshipProjection.type().isPresent()) {
                        mappingsBuilder.addMapping(RelationshipProjectionMapping.all(relationshipProjection.projection()));
                    } else {
                        String elementIdentifier = entry.getKey().name;
                        String typeName = relationshipProjection.type().get();
                        Projection projection = relationshipProjection.projection();
                        int typeId = tokenRead.relationshipType(typeName);

                        RelationshipProjectionMapping typeMapping = RelationshipProjectionMapping.of(
                            elementIdentifier,
                            typeName,
                            projection,
                            typeId
                        );
                        mappingsBuilder.addMapping(typeMapping);
                    }
                }
            }
        }
        RelationshipProjectionMappings relationshipProjectionMappings = mappingsBuilder.build();

        ResolvedPropertyMappings nodeProperties = loadPropertyMapping(tokenRead, setup.nodePropertyMappings());
        ResolvedPropertyMappings relProperties = loadPropertyMapping(tokenRead, setup.relationshipPropertyMappings());

        long nodeCount = nodeLabelIds.stream().mapToLong(dataRead::countsForNode).sum();
        final long allNodesCount = InternalReadOps.getHighestPossibleNodeCount(dataRead, api);
        // TODO: this will double count relationships between distinct labels
        Map<String, Long> relationshipCounts = relationshipProjectionMappings
            .stream()
            .filter(RelationshipProjectionMapping::exists)
            .collect(Collectors.toMap(
                RelationshipProjectionMapping::elementIdentifier,
                relationshipProjectionMapping -> nodeLabelIds.stream()
                    .mapToLong(nodeLabelId -> maxRelCountForLabelAndType(
                        dataRead,
                        nodeLabelId,
                        relationshipProjectionMapping.typeId()
                    )).sum()
            ));
        long maxRelCount = relationshipCounts.values().stream().mapToLong(Long::longValue).sum();

        return ImmutableGraphDimensions.builder()
                .nodeCount(nodeCount)
                .highestNeoId(allNodesCount)
                .maxRelCount(maxRelCount)
                .relationshipCounts(relationshipCounts)
                .nodeLabelIds(nodeLabelIds.longSet())
                .nodeProperties(nodeProperties)
                .relationshipProjectionMappings(relationshipProjectionMappings)
                .relationshipProperties(relProperties)
                .build();
    }

    private boolean loadAnyRelationshipType() {
        return StringUtils.isEmpty(setup.relationshipType());
    }

    private ResolvedPropertyMappings loadPropertyMapping(TokenRead tokenRead, PropertyMappings propertyMappings) {
        ResolvedPropertyMappings.Builder builder = ResolvedPropertyMappings.builder();
        for (PropertyMapping mapping : propertyMappings) {
            String propertyName = mapping.neoPropertyKey();
            int key = propertyName != null ? tokenRead.propertyKey(propertyName) : TokenRead.NO_TOKEN;
            builder.addMapping(mapping.resolveWith(key));
        }
        return builder.build();
    }

    private static long maxRelCountForLabelAndType(Read dataRead, int labelId, int id) {
        return Math.max(
                dataRead.countsForRelationshipWithoutTxState(labelId, id, Read.ANY_LABEL),
                dataRead.countsForRelationshipWithoutTxState(Read.ANY_LABEL, id, labelId)
        );
    }

    static class NodeLabelIds {
        Set<Integer> ids;

        NodeLabelIds() {
            this.ids = new HashSet<>();
        }

        Stream<Integer> stream() {
            return ids.isEmpty() ? Stream.of(Read.ANY_LABEL) : ids.stream();
        }

        LongSet longSet() {
            LongSet longSet = new LongHashSet(ids.size());
            ids.forEach(longSet::add);
            return longSet;
        }
    }

}
