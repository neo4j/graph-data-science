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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.ElementProjection;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.compat.InternalReadOps;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

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

        final TokenElementIdentifierMappings<NodeLabel> labelTokenNodeLabelMappings = new TokenElementIdentifierMappings<>();
        if (readTokens) {
            setup.nodeProjections()
                .projections()
                .forEach((key, value) -> {
                    int labelId = value.projectAll() ? ANY_LABEL : tokenRead.nodeLabel(value.label());
                    labelTokenNodeLabelMappings.put(labelId, key);
                });
        }

        final TokenElementIdentifierMappings<RelationshipType> typeTokenRelTypeMappings = new TokenElementIdentifierMappings<>();
        if (readTokens) {
            setup.relationshipProjections()
                .projections()
                .forEach((key, value) -> {
                    int typeId = value.projectAll() ? ANY_RELATIONSHIP_TYPE : tokenRead.relationshipType(value.type());
                    typeTokenRelTypeMappings.put(typeId, key);
                });
        }



        Map<String, Integer> nodePropertyTokens = loadPropertyTokens(graphCreateConfig.nodeProjections().projections(), tokenRead);
        Map<String, Integer> relationshipPropertyTokens = loadPropertyTokens(graphCreateConfig.relationshipProjections().projections(), tokenRead);

        long nodeCount = labelTokenNodeLabelMappings.keyStream()
            .mapToLong(dataRead::countsForNode)
            .sum();
        final long allNodesCount = InternalReadOps.getHighestPossibleNodeCount(dataRead, api);
        long finalNodeCount = labelTokenNodeLabelMappings.keys().contains(ANY_LABEL)
            ? allNodesCount
            : Math.min(nodeCount, allNodesCount);

        // TODO: this will double count relationships between distinct labels
        Map<RelationshipType, Long> relationshipCounts = getRelationshipCountsByType(
            dataRead,
            labelTokenNodeLabelMappings,
            typeTokenRelTypeMappings
        );
        long maxRelCount = relationshipCounts.values().stream().mapToLong(Long::longValue).sum();

        return ImmutableGraphDimensions.builder()
                .nodeCount(finalNodeCount)
                .highestNeoId(allNodesCount)
                .maxRelCount(maxRelCount)
                .relationshipCounts(relationshipCounts)
                .nodeLabelIds(labelTokenNodeLabelMappings.keys())
                .relationshipTypeIds(typeTokenRelTypeMappings.keys())
                .labelTokenNodeLabelMapping(labelTokenNodeLabelMappings.mappings())
                .typeTokenRelationshipTypeMapping(typeTokenRelTypeMappings.mappings())
                .nodePropertyTokens(nodePropertyTokens)
                .relationshipPropertyTokens(relationshipPropertyTokens)
                .build();
    }

    private Map<String, Integer> loadPropertyTokens(Map<? extends ElementIdentifier, ? extends ElementProjection> projections, TokenRead tokenRead) {
        return projections
            .entrySet()
            .stream()
            .flatMap(nodeProjections -> nodeProjections.getValue().properties().stream())
            .collect(Collectors.toMap(
                PropertyMapping::neoPropertyKey,
                propertyMapping -> propertyMapping.neoPropertyKey() != null ? tokenRead.propertyKey(propertyMapping.neoPropertyKey()) : NO_TOKEN,
                (sameKey1, sameKey2) -> sameKey1
            ));
    }

    @NotNull
    private Map<RelationshipType, Long> getRelationshipCountsByType(
        Read dataRead,
        TokenElementIdentifierMappings<NodeLabel> labelTokenNodeLabelMappings,
        TokenElementIdentifierMappings<RelationshipType> typeTokenRelTypeMappings
    ) {
        Map<RelationshipType, Long> relationshipCountsByType = new HashMap<>();
        typeTokenRelTypeMappings
            .forEach((typeToken, relationshipTypes) ->
                relationshipTypes.forEach(relationshipType -> {
                    if (typeToken != NO_SUCH_RELATIONSHIP_TYPE) {
                        long numberOfRelationships = labelTokenNodeLabelMappings
                            .keyStream()
                            .mapToLong(labelToken -> maxRelCountForLabelAndType(dataRead, labelToken, typeToken)).sum();

                        relationshipCountsByType.put(relationshipType, numberOfRelationships);
                    }
                })
            );

        return relationshipCountsByType;
    }

    private static long maxRelCountForLabelAndType(Read dataRead, int labelId, int id) {
        return Math.max(
            dataRead.countsForRelationshipWithoutTxState(labelId, id, TokenRead.ANY_LABEL),
            dataRead.countsForRelationshipWithoutTxState(TokenRead.ANY_LABEL, id, labelId)
        );
    }

    static class TokenElementIdentifierMappings<T extends ElementIdentifier> {
        private final IntObjectMap<List<T>> mappings;

        TokenElementIdentifierMappings() {
            this.mappings = new IntObjectHashMap<>();
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
                ? Stream.of(TokenRead.ANY_LABEL)
                : StreamSupport.stream(keys().spliterator(), false).map(cursor -> (int) cursor.value);
        }

        void forEach(BiConsumer<Integer, List<T>> consumer) {
            keyStream().forEach(key -> consumer.accept(key, mappings.get(key)));
        }

        IntObjectMap<List<T>> mappings() {
            return this.mappings;
        }

        void put(int key, T value) {
            if (!this.mappings.containsKey(key)) {
                this.mappings.put(key, new ArrayList<>());
            }
            this.mappings.get(key).add(value);
        }

    }
}
