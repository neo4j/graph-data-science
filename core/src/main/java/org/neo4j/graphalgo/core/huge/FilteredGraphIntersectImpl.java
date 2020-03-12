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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.core.loading.IdMap;

/**
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link org.neo4j.graphalgo.api.RelationshipIterator}s.
 */

class FilteredGraphIntersectImpl implements RelationshipIntersect {

    private final IdMap filteredIdMap;
    private final RelationshipIntersect wrappedRelationshipIntersect;

    FilteredGraphIntersectImpl(IdMap filteredIdMap, RelationshipIntersect wrappedRelationshipIntersect) {
        this.filteredIdMap = filteredIdMap;
        this.wrappedRelationshipIntersect = wrappedRelationshipIntersect;
    }

    @Override
    public void intersectAll(long nodeIdA, IntersectionConsumer consumer) {
        wrappedRelationshipIntersect.intersectAll(filteredIdMap.toOriginalNodeId(nodeIdA), (a, b, c) -> {
            if (filteredIdMap.contains(a) && filteredIdMap.contains(b) && filteredIdMap.contains(c)) {
                consumer.accept(
                    filteredIdMap.toMappedNodeId(a),
                    filteredIdMap.toMappedNodeId(b),
                    filteredIdMap.toMappedNodeId(c)
                );
            }
        });
    }
}
