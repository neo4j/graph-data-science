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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.core.concurrency.Concurrency;

/**
 * A {@link org.neo4j.gds.api.nodes.NodeTranslator} that can produce a filtered
 * sub-translator. The construction is translator-specific.
 */
public interface FilterableNodeTranslator extends NodeTranslator {

    /**
     * Builds a translator over the subset of this translator's mapped id space
     * selected by the given {@code unionBitSet}. In the returned translator, the
     * "original" space is this translator's mapped id space (root mapped ids) and
     * the "mapped" id space is the consecutive filtered id space.
     */
    NodeTranslator filteredNodeTranslator(BitSet unionBitSet, Concurrency concurrency);
}
