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

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongSet;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ValueClass
public interface GraphDimensions {

    long nodeCount();

    @Value.Default
    default long highestNeoId() {
        return nodeCount();
    }

    @Value.Default
    default long maxRelCount() {
        return 0L;
    }

    @Value.Default
    default Map<RelationshipType, Long> relationshipCounts() {
        return Collections.emptyMap();
    }

    @Nullable
    LongSet nodeLabelIds();

    @Nullable
    LongSet relationshipTypeIds();

    @Nullable
    IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping();

    @Nullable
    IntObjectMap<List<RelationshipType>> typeTokenRelationshipTypeMapping();

    @Value.Default
    default Map<String, Integer> nodePropertyTokens() {
        return Collections.emptyMap();
    }

    @Value.Default
    default Map<String, Integer> relationshipPropertyTokens() {
        return Collections.emptyMap();
    }

    default Set<NodeLabel> nodeLabels() {
        var nodeLabels = new HashSet<NodeLabel>();
        if (labelTokenNodeLabelMapping() != null) {
            for (var tokenToLabels : labelTokenNodeLabelMapping()) {
                nodeLabels.addAll(tokenToLabels.value);
            }
        }
        return nodeLabels;
    }

//    default Aggregation[] aggregations(Aggregation defaultAggregation) {
//        Aggregation[] aggregations = relationshipProperties().stream()
//            .map(property -> property.aggregation() == Aggregation.DEFAULT
//                ? Aggregation.NONE
//                : property.aggregation()
//            )
//            .toArray(Aggregation[]::new);
//        // TODO: backwards compat code
//        if (aggregations.length == 0) {
//            Aggregation aggregation = defaultAggregation == Aggregation.DEFAULT
//                ? Aggregation.NONE
//                : defaultAggregation;
//            aggregations = new Aggregation[]{aggregation};
//        }
//        return aggregations;
//    }
}
