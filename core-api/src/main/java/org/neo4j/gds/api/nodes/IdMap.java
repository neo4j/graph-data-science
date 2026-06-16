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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.BatchNodeIterable;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.NodeIterator;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Collection;
import java.util.Optional;

/**
 * Usually the IdMap is used to map between neo4j
 * node ids and consecutive mapped node ids.
 */
public interface IdMap extends NodeTranslator, NodeLabels, PartialIdMap, NodeIterator, BatchNodeIterable {

    /**
     * Defines the lower bound of mapped ids
     */
    long START_NODE_ID = 0;


    /**
     * Used for IdMap implementations that do not require a type definition.
     */
    String NO_TYPE = "unsupported";

    /**
     * Maps a filtered mapped node id to its root mapped node id.
     * This is necessary for nested (filtered) id mappings.
     *
     * If this mapping is a nested mapping, this method
     * returns the root mapped node id of the parent mapping.
     * For the root mapping this method returns the given
     * node id.
     */
    long toRootNodeId(long mappedNodeId);

    /**
     * Returns the original node mapping if the current node mapping is filtered, otherwise
     * it returns itself.
     */
    IdMap rootIdMap();

    /**
     * Returns an id map that maps between this id map's id space (the root
     * id space) and a compact id space containing only the nodes that have
     * at least one of the given node labels.
     *
     * Returns an empty Optional if no filtered id map is necessary, i.e.,
     * if there is no label information or the given labels cover all nodes
     * of this id map. Callers are expected to fall back to this id map in
     * that case.
     */
    default Optional<FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, Concurrency concurrency) {
        throw new UnsupportedOperationException("This node mapping does not support label filtering");
    }

}
