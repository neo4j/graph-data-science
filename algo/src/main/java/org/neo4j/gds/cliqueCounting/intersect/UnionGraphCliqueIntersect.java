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
package org.neo4j.gds.cliqueCounting.intersect;

import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.huge.CompositeAdjacencyList;

public final class UnionGraphCliqueIntersect implements CliqueAdjacency {

    private final LongToLongFunction fromFilteredIdFunction;
    private final CompositeAdjacencyList compositeAdjacencyList;

    public UnionGraphCliqueIntersect(
        LongToLongFunction fromFilteredIdFunction,
        CompositeAdjacencyList compositeAdjacencyList
    ) {
        this.fromFilteredIdFunction = fromFilteredIdFunction;
        this.compositeAdjacencyList = compositeAdjacencyList;
    }

    @Override
    public AdjacencyCursor createCursor(long node) {
        return compositeAdjacencyList.adjacencyCursor(fromFilteredIdFunction.applyAsLong(node));
    }
}
