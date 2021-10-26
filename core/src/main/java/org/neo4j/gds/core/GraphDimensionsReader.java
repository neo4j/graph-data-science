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
package org.neo4j.gds.core;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.utils.StatementFunction;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.core.GraphDimensions.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_RELATIONSHIP_TYPE;

public abstract class GraphDimensionsReader<T extends GraphCreateConfig> extends StatementFunction<GraphDimensions> {

    private final IdGeneratorFactory idGeneratorFactory;
    protected T graphCreateConfig;

    GraphDimensionsReader(
        TransactionContext tx,
        T graphCreateConfig,
        IdGeneratorFactory idGeneratorFactory
    ) {
        super(tx);
        this.graphCreateConfig = graphCreateConfig;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public GraphDimensions apply(KernelTransaction transaction) throws RuntimeException {
        TokenRead tokenRead = transaction.tokenRead();
        Read dataRead = transaction.dataRead();

        final TokenElementIdentifierMappings<NodeLabel> labelTokenNodeLabelMappings = getNodeLabelTokens(tokenRead);

        final TokenElementIdentifierMappings<RelationshipType> typeTokenRelTypeMappings = getRelationshipTypeTokens(tokenRead);

        Map<String, Integer> nodePropertyTokens = loadPropertyTokens(getNodeProjections().projections(), tokenRead);
        Map<String, Integer> relationshipPropertyTokens = loadPropertyTokens(getRelationshipProjections().projections(), tokenRead);

        long nodeCount = labelTokenNodeLabelMappings.keyStream()
            .mapToLong(dataRead::countsForNode)
            .sum();
        final long allNodesCount = Neo4jProxy.getHighestPossibleNodeCount(dataRead, idGeneratorFactory);
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
        long allRelationshipsCount = Neo4jProxy.getHighestPossibleRelationshipCount(dataRead, idGeneratorFactory);

        return ImmutableGraphDimensions.builder()
            .nodeCount(finalNodeCount)
            .highestPossibleNodeCount(allNodesCount)
            .maxRelCount(maxRelCount)
            .relationshipCounts(relationshipCounts)
            .highestRelationshipId(allRelationshipsCount)
            .nodeLabelTokens(labelTokenNodeLabelMappings.keys())
            .relationshipTypeTokens(typeTokenRelTypeMappings.keys())
            .tokenNodeLabelMapping(labelTokenNodeLabelMappings.mappings())
            .tokenRelationshipTypeMapping(typeTokenRelTypeMappings.mappings())
            .nodePropertyTokens(nodePropertyTokens)
            .relationshipPropertyTokens(relationshipPropertyTokens)
            .build();
    }

    protected abstract TokenElementIdentifierMappings<NodeLabel> getNodeLabelTokens(TokenRead tokenRead);

    protected abstract TokenElementIdentifierMappings<RelationshipType> getRelationshipTypeTokens(TokenRead tokenRead);

    protected abstract NodeProjections getNodeProjections();

    protected abstract RelationshipProjections getRelationshipProjections();

    protected Map<String, Integer> loadPropertyTokens(Map<? extends ElementIdentifier, ? extends ElementProjection> projectionMapping, TokenRead tokenRead) {
        return projectionMapping
            .values()
            .stream()
            .flatMap(projections -> projections.properties().stream())
            .collect(Collectors.toMap(
                PropertyMapping::neoPropertyKey,
                propertyMapping -> propertyMapping.neoPropertyKey() != null ? tokenRead.propertyKey(propertyMapping.neoPropertyKey()) : StatementConstants.NO_SUCH_PROPERTY_KEY,
                (sameKey1, sameKey2) -> sameKey1
            ));
    }

    @NotNull
    protected Map<RelationshipType, Long> getRelationshipCountsByType(
        Read dataRead,
        TokenElementIdentifierMappings<NodeLabel> labelTokenNodeLabelMappings,
        TokenElementIdentifierMappings<RelationshipType> typeTokenRelTypeMappings
    ) {
        Map<RelationshipType, Long> relationshipCountsByType = new HashMap<>();
        typeTokenRelTypeMappings
            .forEach((typeToken, relationshipTypes) -> {
                if (typeToken == ANY_RELATIONSHIP_TYPE && relationshipTypes == null) {
                  relationshipTypes = Collections.singletonList(RelationshipType.ALL_RELATIONSHIPS);
                }
                relationshipTypes.forEach(relationshipType -> {
                    if (typeToken != NO_SUCH_RELATIONSHIP_TYPE) {
                        long numberOfRelationships = labelTokenNodeLabelMappings
                            .keyStream()
                            .mapToLong(labelToken -> maxRelCountForLabelAndType(dataRead, labelToken, typeToken)).sum();

                        relationshipCountsByType.put(relationshipType, numberOfRelationships);
                    }
                });
            });

        return relationshipCountsByType;
    }

    private static long maxRelCountForLabelAndType(Read dataRead, int labelId, int id) {
        return Math.max(
            dataRead.countsForRelationshipWithoutTxState(labelId, id, ANY_LABEL),
            dataRead.countsForRelationshipWithoutTxState(ANY_LABEL, id, labelId)
        );
    }

    static class TokenElementIdentifierMappings<T extends ElementIdentifier> {
        private final IntObjectMap<List<T>> mappings;
        private final int allToken;

        TokenElementIdentifierMappings(int allToken) {
            this.allToken = allToken;
            this.mappings = new IntObjectHashMap<>();
        }

        LongSet keys() {
            LongSet keySet = new LongHashSet(mappings.keys().size());
            boolean allNodes = StreamSupport.stream(mappings.keys().spliterator(), false)
                .allMatch(cursor -> cursor.value == allToken);
            if (!allNodes) {
                StreamSupport.stream(mappings.keys().spliterator(), false)
                    .forEach(cursor -> keySet.add(cursor.value));
            }
            return keySet;
        }

        Stream<Integer> keyStream() {
            return keys().isEmpty()
                ? Stream.of(allToken)
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
