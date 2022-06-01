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
package org.neo4j.gds.similarity.filtering;

import org.neo4j.gds.api.IdMap;

/**
 * A {@code NodeFilterSpec} is a partially constructed {@link NodeFilter}. Because we cannot fully construct the
 * {@link NodeFilter} from user inputs alone, we parse and validate what we can, and prepare for completing the
 * construction once the rest is available.
 *
 * There are two types of node filters: ones based on a list of node IDs, and ones based on labels.
 * There are therefore two types of node filter specs, accordingly.
 *
 * The spec is created using {@link NodeFilterSpecFactory#create(Object)} and the {@link NodeFilter} is then created
 * using {@link NodeFilterSpec#toNodeFilter(org.neo4j.gds.api.IdMap)}.
 */
public interface NodeFilterSpec {
    NodeFilter toNodeFilter(IdMap idMap);

    NodeFilterSpec noOp = (idMap) -> NodeFilter.noOp;
}
