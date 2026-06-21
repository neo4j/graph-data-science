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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.LabeledIdMap;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;

import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;

public final class ShardedIdMap extends LabeledIdMap {

    private final ShardedLongLongMap idMap;

    ShardedIdMap(ShardedLongLongMap idMap, LabelInformation labelInformation) {
        super(labelInformation, idMap.size());
        this.idMap = idMap;
    }

    @Override
    public String typeId() {
        return "sharded";
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return idMap.toMappedNodeId(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return idMap.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return mappedNodeId;
    }

    @Override
    public boolean containsOriginalId(long originalNodeId) {
        return idMap.contains(originalNodeId);
    }

    @Override
    public long highestOriginalId() {
        return idMap.maxOriginalId();
    }

    @Override
    public IdMap rootIdMap() {
        return this;
    }

    @Override
    public OptionalLong rootNodeCount() {
        return OptionalLong.of(nodeCount());
    }

    @Override
    public Optional<FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, Concurrency concurrency) {
        labelInformation.validateNodeLabelFilter(nodeLabels);

        if (labelInformation.isEmpty()) {
            return Optional.empty();
        }

        // Filtering by all available labels is a no-op; callers fall back to this id map.
        if (nodeLabels.containsAll(labelInformation.availableNodeLabels())) {
            return Optional.empty();
        }

        // The filtered representation (ArrayIdMap on community, BitIdMap on enterprise) is
        // chosen by the IdMapBehavior. ShardedIdMap is in public/core and must NOT build a
        // BitIdMap directly, so it delegates.
        return Optional.of(
            IdMapBehaviorServiceProvider.idMapBehavior()
                .filteredIdMap(this, labelInformation, nodeLabels, concurrency)
        );
    }
}
