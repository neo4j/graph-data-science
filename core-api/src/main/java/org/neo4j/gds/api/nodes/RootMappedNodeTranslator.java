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
package org.neo4j.gds.api.nodes;

/**
 * Translates original node ids via root node ids to the mapped node ids of an id map:
 * originalNodeId -- rootNodeId -- mappedNodeId
 */
final class RootMappedNodeTranslator implements NodeTranslator {

    private final NodeTranslator rootNodeTranslator;
    private final RootNodeMapping rootNodeMapping;
    private final long nodeCount;

    RootMappedNodeTranslator(NodeTranslator rootNodeTranslator, RootNodeMapping rootNodeMapping, long nodeCount) {
        this.rootNodeTranslator = rootNodeTranslator;
        this.rootNodeMapping = rootNodeMapping;
        this.nodeCount = nodeCount;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    /**
     * The highest original node id of the root id map, which is an upper bound
     * for the original node ids of this id map.
     */
    @Override
    public long highestOriginalId() {
        return rootNodeTranslator.highestOriginalId();
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return rootNodeTranslator.toOriginalNodeId(rootNodeMapping.toRootNodeId(mappedNodeId));
    }

    /**
     * If the original node id is not mapped by the root node translator, the lookup
     * must short-circuit to {@link NodeTranslator#NOT_FOUND}, which is not a valid
     * root node id.
     */
    @Override
    public long toMappedNodeId(long originalNodeId) {
        var rootNodeId = rootNodeTranslator.toMappedNodeId(originalNodeId);
        if (rootNodeId == NOT_FOUND) {
            return NOT_FOUND;
        }
        return rootNodeMapping.fromRootNodeId(rootNodeId);
    }

    @Override
    public boolean containsOriginalId(long originalNodeId) {
        return toMappedNodeId(originalNodeId) != NOT_FOUND;
    }

    @Override
    public String typeId() {
        return rootNodeTranslator.typeId();
    }
}
