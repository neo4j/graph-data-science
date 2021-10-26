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
package org.neo4j.gds.core;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.annotations.service.Service;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.core.loading.IdMappingAllocator;
import org.neo4j.gds.core.loading.InternalIdMappingBuilder;
import org.neo4j.gds.core.loading.InternalIdMappingBuilderFactory;
import org.neo4j.gds.core.loading.NodeMappingBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

import java.util.Optional;

@Service
public interface IdMapBehavior {

    Pair<InternalIdMappingBuilderFactory<? extends InternalIdMappingBuilder<?>, ?>, NodeMappingBuilder> create(
        boolean maxIdKnown, GraphLoaderContext loaderContext
    );

    Pair<InternalIdMappingBuilder<? extends IdMappingAllocator>, NodeMappingBuilder.Capturing> create(
        boolean maxIdKnown,
        long maxOriginalId,
        AllocationTracker allocationTracker,
        Optional<Long> nodeCount
    );

    int priority();

    MemoryEstimation memoryEstimation();
}
