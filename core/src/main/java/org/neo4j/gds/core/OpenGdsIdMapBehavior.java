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

import org.neo4j.gds.core.loading.IdMap;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.InternalHugeIdMappingBuilder;
import org.neo4j.gds.core.loading.InternalIdMappingBuilderFactory;
import org.neo4j.gds.core.loading.NodeMappingBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

import java.util.Optional;

public class OpenGdsIdMapBehavior implements IdMapBehavior {

    public interface InternalHugeIdMappingBuilderFactory extends InternalIdMappingBuilderFactory<InternalHugeIdMappingBuilder, InternalHugeIdMappingBuilder.BulkAdder> {}

    @Override
    public DefaultIdMapBehaviour create(AllocationTracker allocationTracker) {
        InternalHugeIdMappingBuilderFactory idMappingBuilderFactory =
            dimensions -> InternalHugeIdMappingBuilder.of(
                dimensions.nodeCount(),
                allocationTracker
            );

        return ImmutableDefaultIdMapBehaviour.builder()
            .idMappingBuilderFactory(idMappingBuilderFactory)
            .nodeMappingBuilder(nodeMappingBuilder())
            .build();
    }

    @Override
    public CapturingIdMapBehaviour create(
        Optional<Long> maxOriginalId,
        Optional<Long> nodeCount, AllocationTracker allocationTracker
    ) {
        long capacity = maxOriginalId
            .map(maxId -> maxId + 1)
            .orElseGet(() -> nodeCount.orElseThrow(() -> new IllegalArgumentException(
                "Either `maxOriginalId` or `nodeCount` must be set")));
        var idMapBuilder = InternalHugeIdMappingBuilder.of(capacity, allocationTracker);
        var capturing = nodeMappingBuilder().capture(idMapBuilder);

        return ImmutableCapturingIdMapBehaviour.builder()
            .idMappingBuilder(idMapBuilder)
            .nodeMappingBuilder(capturing)
            .build();
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return IdMap.memoryEstimation();
    }

    public NodeMappingBuilder nodeMappingBuilder() {
        return (idMapBuilder, labelInformationBuilder, highestNeoId, concurrency, checkDuplicateIds, allocationTracker) -> {
            if (checkDuplicateIds) {
                return IdMapBuilder.buildChecked(
                    (InternalHugeIdMappingBuilder) idMapBuilder,
                    labelInformationBuilder,
                    highestNeoId,
                    concurrency,
                    allocationTracker
                );
            } else {
                return IdMapBuilder.build(
                    (InternalHugeIdMappingBuilder) idMapBuilder,
                    labelInformationBuilder,
                    highestNeoId,
                    concurrency,
                    allocationTracker
                );
            }
        };
    }
}
