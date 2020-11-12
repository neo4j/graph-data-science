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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;

import java.util.Map;
import java.util.stream.Collectors;

public final class IdMapBuilder {

    public static IdMap build(
        LokiInternalIdMappingBuilder idMapBuilder,
        Map<NodeLabel, HugeAtomicBitSet> labelInformation,
        AllocationTracker tracker
    ) {
        SparseLongArray graphIds = idMapBuilder.build();

        var convertedLabelInformation = labelInformation.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().toBitSet()
        ));

        return new IdMap(
            graphIds,
            convertedLabelInformation,
            idMapBuilder.size(),
            tracker
        );
    }

    static IdMap buildChecked(
        LokiInternalIdMappingBuilder idMapBuilder,
        Map<NodeLabel, HugeAtomicBitSet> labelInformation,
        AllocationTracker tracker
    ) throws DuplicateNodeIdException {
        SparseLongArray graphIds = idMapBuilder.build();

        var convertedLabelInformation = labelInformation.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().toBitSet()
        ));

        return new IdMap(graphIds, convertedLabelInformation, idMapBuilder.size(), tracker);
    }

    private IdMapBuilder() {
    }
}
