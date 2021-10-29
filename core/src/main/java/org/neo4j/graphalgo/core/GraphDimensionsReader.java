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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.RelationshipProjectionMappings;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.compat.InternalReadOps;
import org.neo4j.graphalgo.compat.StatementConstantsProxy;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.ANY_LABEL;

public final class GraphDimensionsReader extends StatementFunction<GraphDimensions> {
    private final GraphSetup setup;
    private final GraphCreateConfig graphCreateConfig;
    private final boolean readTokens;

    public GraphDimensionsReader(
            GraphDatabaseAPI api,
            GraphSetup setup,
            GraphCreateConfig graphCreateConfig,
            boolean readTokens) {
        super(api);
        this.setup = setup;
        this.graphCreateConfig = graphCreateConfig;
        this.readTokens = readTokens;
    }

    @Override
    public GraphDimensions apply(KernelTransaction transaction) throws RuntimeException {
        TokenRead tokenRead = transaction.tokenRead();
        Read dataRead = transaction.dataRead();

        final LabelTokenNodeLabelMappings labelTokenNodeLabelMappings = new LabelTokenNodeLabelMappings();
        if (readTokens) {
            setup.nodeProjections()
                .projections()
                .forEach((key, value) -> {
                    long labelId = value.projectAll() ? ANY_LABEL : (long) tokenRead.nodeLabel(value.label());
                    labelTokenNodeLabelMappings.put(labelId, key);
                });
        }

        RelationshipProjectionMappings.Builder mappingsBuilder = new RelationshipProjectionMappings.Builder();
        if (readTokens) {
            setup.relationshipProjections().projections().forEach((relationshipType, relationshipProjection) -> {

                String typeName = relationshipProjection.type();
                Orientation orientation = relationshipProjection.orientation();

                RelationshipProjectionMapping mapping = relationshipProjection.projectAll()
                    ? RelationshipType.ALL_RELATIONSHIPS.equals(relationshipType)
                        ? RelationshipProjectionMapping.all(orientation)
                        : RelationshipProjectionMapping.all(relationshipType.name(), orientation)
                    : RelationshipProjectionMapping.of(
                        relationshipType,
                        typeName,
                        orientation,
                        tokenRead.relationshipType(typeName)
                    );
                mappingsBuilder.addMapping(mapping);
            });
        }

        RelationshipProjectionMappings relationshipProjectionMappings = mappingsBuilder.build();
        Map<String, Integer> nodePropertyTokens = loadNodePropertyTokens(tokenRead);
        ResolvedPropertyMappings relProperties = loadPropertyMapping(tokenRead, setup.relationshipPropertyMappings());

        // Every node will be counted once for every label it has.
        long nodeCount = labelTokenNodeLabelMappings.keyStream()
            .mapToLong(dataRead::countsForNode)
            .sum();
        final long highestPossibleNodeCount = InternalReadOps.getHighestPossibleNodeCount(dataRead, api);
        // This is also an upper bound on the node count.
        long finalNodeCount = labelTokenNodeLabelMappings.keys().contains(ANY_LABEL)
            ? highestPossibleNodeCount
            : Math.min(nodeCount, highestPossibleNodeCount);

        // TODO: this will double count relationships between distinct labels
        Map<RelationshipType, Long> relationshipCounts = relationshipProjectionMappings
            .stream()
            .filter(RelationshipProjectionMapping::exists)
            .collect(Collectors.toMap(
                RelationshipProjectionMapping::relationshipType,
                relationshipProjectionMapping ->
                    labelTokenNodeLabelMappings
                        .keyStream()
                        .mapToLong(labelId -> maxRelCountForLabelAndType(
                            dataRead,
                            labelId,
                            relationshipProjectionMapping.typeId()
                        )).sum()
            ));
        long maxRelCount = relationshipCounts.values().stream().mapToLong(Long::longValue).sum();

        return ImmutableGraphDimensions.builder()
                .nodeCount(finalNodeCount)
                .highestNeoId(highestPossibleNodeCount)
                .maxRelCount(maxRelCount)
                .relationshipCounts(relationshipCounts)
                .nodeLabelIds(labelTokenNodeLabelMappings.keys())
                .labelTokenNodeLabelMapping(labelTokenNodeLabelMappings.mappings())
                .nodePropertyTokens(nodePropertyTokens)
                .relationshipProjectionMappings(relationshipProjectionMappings)
                .relationshipProperties(relProperties)
                .build();
    }

    private Map<String, Integer> loadNodePropertyTokens(TokenRead tokenRead) {
        return graphCreateConfig.nodeProjections()
            .projections()
            .entrySet()
            .stream()
            .flatMap(nodeProjections -> nodeProjections.getValue().properties().stream())
            .collect(Collectors.toMap(
                PropertyMapping::neoPropertyKey,
                propertyMapping -> propertyMapping.neoPropertyKey() != null ? tokenRead.propertyKey(propertyMapping.neoPropertyKey()) : TokenRead.NO_TOKEN,
                (sameKey1, sameKey2) -> sameKey1
            ));
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
                dataRead.countsForRelationshipWithoutTxState(labelId, id, StatementConstantsProxy.ANY_LABEL),
                dataRead.countsForRelationshipWithoutTxState(StatementConstantsProxy.ANY_LABEL, id, labelId)
        );
    }

    static class LabelTokenNodeLabelMappings {
        private final LongObjectMap<List<NodeLabel>> mappings;

        LabelTokenNodeLabelMappings() {
            this.mappings = new LongObjectHashMap<>();
        }

        LongSet keys() {
            LongSet keySet = new LongHashSet(mappings.keys().size());
            boolean allNodes = StreamSupport.stream(mappings.keys().spliterator(), false)
                .allMatch(cursor -> cursor.value == ANY_LABEL);
            if (!allNodes) {
                StreamSupport.stream(mappings.keys().spliterator(), false)
                    .forEach(cursor -> keySet.add(cursor.value));
            }
            return keySet;
        }

        Stream<Integer> keyStream() {
            return keys().isEmpty()
                ? Stream.of(StatementConstantsProxy.ANY_LABEL)
                : StreamSupport.stream(keys().spliterator(), false).map(cursor -> (int) cursor.value);
        }

        LongObjectMap<List<NodeLabel>> mappings() {
            return this.mappings;
        }

        void put(long key, NodeLabel value) {
            if (!this.mappings.containsKey(key)) {
                this.mappings.put(key, new ArrayList<>());
            }
            this.mappings.get(key).add(value);
        }

    }
}
