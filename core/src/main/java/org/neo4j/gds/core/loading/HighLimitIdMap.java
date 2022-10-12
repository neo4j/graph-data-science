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
import org.neo4j.gds.api.IdMapAdapter;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;

import java.util.Collection;
import java.util.Optional;

public class HighLimitIdMap extends IdMapAdapter {

    private final ShardedLongLongMap highToLowIdSpace;

    public HighLimitIdMap(ShardedLongLongMap intermediateIdMap, IdMap internalIdMap) {
        super(internalIdMap);
        this.highToLowIdSpace = intermediateIdMap;
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return highToLowIdSpace.toOriginalNodeId(super.toOriginalNodeId(mappedNodeId));
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return super.toMappedNodeId(this.highToLowIdSpace.toMappedNodeId(originalNodeId));
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return highToLowIdSpace.toOriginalNodeId(super.toRootNodeId(mappedNodeId));
    }

    @Override
    public boolean contains(long originalNodeId) {
        return super.contains(highToLowIdSpace.toMappedNodeId(originalNodeId));
    }

    @Override
    public long highestOriginalId() {
        return highToLowIdSpace.maxOriginalId();
    }

    @Override
    public Optional<FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        return super.withFilteredLabels(nodeLabels, concurrency)
            .map(filteredIdMap -> new FilteredHighLimitIdMap(this.highToLowIdSpace, filteredIdMap));
    }

    static final class FilteredHighLimitIdMap extends HighLimitIdMap implements FilteredIdMap {

        private final FilteredIdMap filteredIdMap;

        FilteredHighLimitIdMap(ShardedLongLongMap intermediateIdMap, FilteredIdMap filteredIdMap) {
            super(intermediateIdMap, filteredIdMap);
            this.filteredIdMap = filteredIdMap;
        }

        @Override
        public long toFilteredNodeId(long rootNodeId) {
            return filteredIdMap.toFilteredNodeId(rootNodeId);
        }

        @Override
        public boolean containsRootNodeId(long rootNodeId) {
            return filteredIdMap.containsRootNodeId(rootNodeId);
        }
    }
}
