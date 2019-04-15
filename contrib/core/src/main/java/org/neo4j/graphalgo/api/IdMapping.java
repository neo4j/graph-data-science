/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.api;

/**
 * bidirectional mapping between long neo4j-nodeId and
 * temporary int graph-nodeId.
 *
 * @author mknblch
 */
public interface IdMapping {

    /**
     * defines the lower bound of mapped node ids
     * TODO: function?
     */
    int START_NODE_ID = 0;

    /**
     * Map neo4j nodeId to inner nodeId
     * TODO rename?
     */
    int toMappedNodeId(long nodeId);

    /**
     * Map inner nodeId back to original nodeId
     */
    long toOriginalNodeId(int nodeId);

    /**
     * Returns true iff the nodeId is mapped, otherwise false
     */
    boolean contains(long nodeId);

    /**
     * Count of nodes.
     */
    long nodeCount();
}
