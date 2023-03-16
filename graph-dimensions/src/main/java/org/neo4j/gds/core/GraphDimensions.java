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

import com.carrotsearch.hppc.IntObjectAssociativeContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@ValueClass
public interface GraphDimensions {

    int ANY_LABEL = -1;
    int ANY_RELATIONSHIP_TYPE = -1;
    int NO_SUCH_LABEL = -2;
    int NO_SUCH_RELATIONSHIP_TYPE = -2;
    int IGNORE = -4;

    long nodeCount();

    static ImmutableGraphDimensions.Builder builder() {
        return ImmutableGraphDimensions.builder();
    }

    @Value.Default
    default long highestPossibleNodeCount() {
        return nodeCount();
    }

    @Value.Default
    default long relCountUpperBound() {
        return 0L;
    }

    @Value.Default
    default Map<RelationshipType, Long> relationshipCounts() {
        return Collections.emptyMap();
    }

    @Value.Default
    //Upper bound because due to limitations in the kernel API we might count relationships we end up not loading.
    default long highestRelationshipId() {
        return relCountUpperBound();
    }

    @Nullable
    LongSet nodeLabelTokens();

    @Nullable
    LongSet relationshipTypeTokens();

    @Nullable
    IntObjectMap<List<NodeLabel>> tokenNodeLabelMapping();

    @Value.Derived
    default Collection<NodeLabel> availableNodeLabels() {
        var nodeLabelsIterator = Optional.ofNullable(tokenNodeLabelMapping())
            .map(IntObjectAssociativeContainer::values)
            .map(Iterable::spliterator)
            .orElseGet(Spliterators::emptySpliterator);

        return StreamSupport.stream(
                nodeLabelsIterator,
                false
            )
            .flatMap(cursor -> cursor.value.stream())
            .collect(Collectors.toSet());
    }

    @Value.Derived
    default Collection<NodeLabel> starNodeLabelMappings() {
        return Optional.ofNullable(tokenNodeLabelMapping())
            .orElseGet(IntObjectHashMap::new)
            .getOrDefault(ANY_LABEL, List.of());
    }

    @Nullable
    IntObjectMap<List<RelationshipType>> tokenRelationshipTypeMapping();

    @Value.Derived
    default long averageDegree() {
        return nodeCount() == 0 ? 0 : relCountUpperBound() / nodeCount();
    }

    @Value.Derived
    @Nullable
    default ObjectIntMap<RelationshipType> relationshipTypeTokenMapping() {
        if (tokenRelationshipTypeMapping() == null) {
            return null;
        }

        ObjectIntMap<RelationshipType> relationshipTypeTypeTokenMapping = new ObjectIntHashMap<>();
        tokenRelationshipTypeMapping().forEach((Consumer<? super IntObjectCursor<List<RelationshipType>>>) cursor -> {
            var typeToken = cursor.key;
            var relationshipTypes = cursor.value;
            relationshipTypes.forEach(relationshipType -> relationshipTypeTypeTokenMapping.put(relationshipType, typeToken));
        });

        return relationshipTypeTypeTokenMapping;
    }

    @Value.Default
    default Map<String, Integer> nodePropertyTokens() {
        return Collections.emptyMap();
    }

    @Value.Default
    default DimensionsMap nodePropertyDimensions() {
        return new DimensionsMap(Map.of());
    }

    @Value.Default
    default Map<String, Integer> relationshipPropertyTokens() {
        return Collections.emptyMap();
    }

    @Value.Default
    default int estimationNodeLabelCount() {
        var nodeLabels = new HashSet<NodeLabel>();
        var tokenNodeLabelMapping = tokenNodeLabelMapping();
        if (tokenNodeLabelMapping != null) {
            for (var tokenToLabels : tokenNodeLabelMapping) {
                nodeLabels.addAll(tokenToLabels.value);
            }
        }

        return nodeLabels.stream().allMatch(l -> l.equals(NodeLabel.ALL_NODES))
            ? 0
            : nodeLabels.size();
    }


    static GraphDimensions of(long nodeCount) {
        return of(nodeCount, 0);
    }

    static GraphDimensions of(long nodeCount, long relationshipCount) {
        return ImmutableGraphDimensions.builder()
            .nodeCount(nodeCount)
            .relationshipCounts(Map.of(RelationshipType.ALL_RELATIONSHIPS, relationshipCount))
            .relCountUpperBound(relationshipCount)
            .build();
    }

    default long estimatedRelCount(List<String> relationshipTypeNames) {
        if (!(relationshipTypeNames.contains(ElementProjection.PROJECT_ALL))) {
            Map<RelationshipType, Long> relCounts = relationshipCounts();
            List<RelationshipType> relationshipTypes = relationshipTypeNames.stream()
                .map(RelationshipType::of)
                .collect(Collectors.toList());

            if (relCounts.keySet().containsAll(relationshipTypes)) {
                return relationshipTypes.stream().mapToLong(relCounts::get).sum();
            }
        }

        return relCountUpperBound();
    }

}
