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
package org.neo4j.graphalgo.core.compress;

import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyProperties;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.CsrListBuilderFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

public interface AdjacencyCompressorFactory<TARGET_PAGE, PROPERTY_PAGE> {

    AdjacencyCompressorBlueprint create(
        long nodeCount,
        CsrListBuilderFactory<TARGET_PAGE, ? extends AdjacencyList, PROPERTY_PAGE, ? extends AdjacencyProperties> csrListBuilderFactory,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation,
        AllocationTracker tracker
    );
}
