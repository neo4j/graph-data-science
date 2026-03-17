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
package org.neo4j.gds.projection;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.immutables.builder.Builder;
import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.compat.InternalReadOps;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.core.GraphDimensions.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_RELATIONSHIP_TYPE;

public final class GraphDimensionsReader extends StatementFunction<GraphDimensions> {
    private final IdGeneratorFactory idGeneratorFactory;
    private final Map<NodeLabel, String> nodeLabelMappings;
    private final Map<RelationshipType, String> relationshipTypeMappings;
    private final List<String> nodeProperties;
    private final List<String> relationshipProperties;

    @Builder.Factory
    static GraphDimensionsReader graphDimensionsReader(
        GraphLoaderContext graphLoaderContext,
        GraphProjectFromStoreConfig graphProjectConfig,
        DependencyResolver dependencyResolver
    ) {
        var nodeLabelMappings = graphProjectConfig.nodeProjections().projections().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().label()
            ));

        var relationshipTypeMappings = graphProjectConfig.relationshipProjections().projections().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().type()
            ));

        var nodeProperties = graphProjectConfig
            .nodeProjections()
            .projections()
            .values()
            .stream()
            .flatMap(projection -> projection.properties().stream())
            .map(PropertyMapping::neoPropertyKey)
            .distinct()
            .toList();

        var relationshipProperties = graphProjectConfig
            .relationshipProjections()
            .projections()
            .values()
            .stream()
            .flatMap(projection -> projection.properties().stream())
            .map(PropertyMapping::neoPropertyKey)
            .distinct()
            .toList();


        return new GraphDimensionsReader(
            graphLoaderContext.transactionContext(),
            dependencyResolver.resolveDependency(IdGeneratorFactory.class),
            nodeLabelMappings,
            relationshipTypeMappings,
            nodeProperties,
            relationshipProperties
        );
    }

    GraphDimensionsReader(
        TransactionContext tx,
        IdGeneratorFactory idGeneratorFactory,
        Map<NodeLabel, String> nodeLabelMappings,
        Map<RelationshipType, String> relationshipTypeMappings,
        List<String> nodeProperties,
        List<String> relationshipProperties
    ) {
        super(tx);
        this.idGeneratorFactory = idGeneratorFactory;
        this.nodeLabelMappings = nodeLabelMappings;
        this.relationshipTypeMappings = relationshipTypeMappings;
        this.nodeProperties = nodeProperties;
        this.relationshipProperties = relationshipProperties;
    }

    @Override
    public GraphDimensions apply(KernelTransaction transaction) throws RuntimeException {
        TokenRead tokenRead = transaction.tokenRead();
        Read dataRead = transaction.dataRead();

        final TokenElementIdentifierMappings<NodeLabel> labelTokenNodeLabelMappings = getNodeLabelTokens(tokenRead);

        final TokenElementIdentifierMappings<RelationshipType> typeTokenRelTypeMappings = getRelationshipTypeTokens(tokenRead);

        Map<String, Integer> nodePropertyTokens = loadPropertyTokens(nodeProperties, tokenRead);
        Map<String, Integer> relationshipPropertyTokens = loadPropertyTokens(relationshipProperties, tokenRead);

        long nodeCount = labelTokenNodeLabelMappings.keyStream()
            .mapToLong(dataRead::estimateCountsForNode)
            .sum();
        final long allNodesCount = InternalReadOps.getHighestPossibleNodeCount(idGeneratorFactory);
        long finalNodeCount = labelTokenNodeLabelMappings.keys().contains(ANY_LABEL)
            ? allNodesCount
            : Math.min(nodeCount, allNodesCount);

        // TODO: this will double count relationships between distinct labels
        Map<RelationshipType, Long> relationshipCounts = getRelationshipCountsByType(
            dataRead,
            labelTokenNodeLabelMappings,
            typeTokenRelTypeMappings
        );
        long relCountUpperBound = relationshipCounts.values().stream().mapToLong(Long::longValue).sum();
        long allRelationshipsCount = dataRead.relationshipsGetCount();

        return ImmutableGraphDimensions.builder()
            .nodeCount(finalNodeCount)
            .highestPossibleNodeCount(allNodesCount)
            .relCountUpperBound(relCountUpperBound)
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

    private TokenElementIdentifierMappings<NodeLabel> getNodeLabelTokens(TokenRead tokenRead) {
        var labelTokenNodeLabelMappings = new TokenElementIdentifierMappings<NodeLabel>(
            ANY_LABEL);
        nodeLabelMappings
            .forEach((nodeLabel, neoLabel) -> {
                var labelToken = neoLabel.equals(PROJECT_ALL) ? ANY_LABEL : getNodeLabelToken(tokenRead, neoLabel);
                labelTokenNodeLabelMappings.put(labelToken, nodeLabel);
            });
        return labelTokenNodeLabelMappings;
    }

    private TokenElementIdentifierMappings<RelationshipType> getRelationshipTypeTokens(TokenRead tokenRead) {
        var typeTokenRelTypeMappings = new TokenElementIdentifierMappings<RelationshipType>(
            ANY_RELATIONSHIP_TYPE);

        relationshipTypeMappings
            .forEach((relType, neoRelType) -> {
                var typeToken = neoRelType.equals(PROJECT_ALL) ? ANY_RELATIONSHIP_TYPE : getRelationshipTypeToken(
                    tokenRead,
                    neoRelType
                );
                typeTokenRelTypeMappings.put(typeToken, relType);
            });
        return typeTokenRelTypeMappings;
    }

    private Map<String, Integer> loadPropertyTokens(
        List<String> properties,
        TokenRead tokenRead
    ) {
        return properties
            .stream()
            .collect(Collectors.toMap(
                Function.identity(),
                property -> property != null ? tokenRead.propertyKey(property) : StatementConstants.NO_SUCH_PROPERTY_KEY,
                (sameKey1, sameKey2) -> sameKey1
            ));
    }

    private Map<RelationshipType, Long> getRelationshipCountsByType(
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
                            .mapToLong(labelToken -> relCountUpperBoundForLabelAndType(dataRead, labelToken, typeToken)).sum();

                        relationshipCountsByType.put(relationshipType, numberOfRelationships);
                    }
                });
            });

        return relationshipCountsByType;
    }

    private static long relCountUpperBoundForLabelAndType(Read dataRead, int labelId, int id) {
        return Math.max(
            dataRead.estimateCountsForRelationships(labelId, id, ANY_LABEL),
            dataRead.estimateCountsForRelationships(ANY_LABEL, id, labelId)
        );
    }

    private int getNodeLabelToken(TokenRead tokenRead, String nodeLabel) {
        int labelToken = tokenRead.nodeLabel(nodeLabel);
        return labelToken == StatementConstants.NO_SUCH_LABEL
            ? NO_SUCH_LABEL
            : labelToken;
    }

    private int getRelationshipTypeToken(TokenRead tokenRead, String relationshipType) {
        int relationshipToken = tokenRead.relationshipType(relationshipType);
        return relationshipToken == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE
            ? NO_SUCH_RELATIONSHIP_TYPE
            : relationshipToken;
    }

    public static class TokenElementIdentifierMappings<T extends ElementIdentifier> {
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

        public void put(int key, T value) {
            if (!this.mappings.containsKey(key)) {
                this.mappings.put(key, new ArrayList<>());
            }
            this.mappings.get(key).add(value);
        }

    }
}
