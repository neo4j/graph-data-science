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

import com.carrotsearch.hppc.LongSet;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.RelationshipTypeMappings;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.Collections;
import java.util.Map;

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
    default Map<String, Long> relationshipCounts() {
        return Collections.emptyMap();
    }

    @Nullable
    LongSet nodeLabelIds();

    @Value.Default
    default ResolvedPropertyMappings nodeProperties() {
        return ResolvedPropertyMappings.empty();
    }

    @Value.Default
    default ResolvedPropertyMappings relationshipProperties() {
        return ResolvedPropertyMappings.empty();
    }

    @Nullable
    RelationshipTypeMappings relationshipTypeMappings();
}
