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
 * Maps between the mapped node ids of an id map and the mapped node ids of its
 * root id map (root node ids). A filtered id map has its own consecutive id space
 * on top of the root id space; for an unfiltered id map, both id spaces are the same.
 * <p>
 * Translating between root node ids and original node ids is not part of this
 * mapping, see {@link org.neo4j.gds.api.GraphStore#nodeTranslator(IdMap)}.
 */
public interface RootNodeMapping {

    /**
     * Mapping for id maps whose mapped node ids are root node ids.
     */
    RootNodeMapping IDENTITY = new RootNodeMapping() {
        @Override
        public long toRootNodeId(long mappedNodeId) {
            return mappedNodeId;
        }

        @Override
        public long fromRootNodeId(long rootNodeId) {
            return rootNodeId;
        }
    };

    /**
     * Maps a mapped node id to its root node id.
     */
    long toRootNodeId(long mappedNodeId);

    /**
     * Maps a root node id to its mapped node id.
     *
     * @return the mapped node id or {@link NodeTranslator#NOT_FOUND} if the root node is not mapped
     */
    long fromRootNodeId(long rootNodeId);

    /**
     * Creates a node translator for an id map with the given root node mapping and node count,
     * where translation between root node ids and original node ids is done by the given
     * root node translator.
     */
    static NodeTranslator nodeTranslator(
        NodeTranslator rootNodeTranslator,
        RootNodeMapping rootNodeMapping,
        long nodeCount
    ) {
        return new RootMappedNodeTranslator(rootNodeTranslator, rootNodeMapping, nodeCount);
    }
}
