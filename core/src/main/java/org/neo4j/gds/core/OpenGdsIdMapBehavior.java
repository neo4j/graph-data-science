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

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.ArrayIdMap;
import org.neo4j.gds.core.loading.ArrayIdMapBuilder;
import org.neo4j.gds.core.loading.GrowingArrayIdMapBuilder;
import org.neo4j.gds.core.loading.HighLimitIdMap;
import org.neo4j.gds.core.loading.HighLimitIdMapBuilder;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.Locale;
import java.util.Optional;

public class OpenGdsIdMapBehavior implements IdMapBehavior {

    @Override
    public IdMapBuilder create(
        Concurrency concurrency,
        Optional<Long> maxOriginalId,
        Optional<Long> nodeCount
    ) {
        return nodeCount.or(() -> maxOriginalId.map(maxId -> maxId + 1))
            .map(capacity -> (IdMapBuilder) ArrayIdMapBuilder.of(capacity))
            .orElseGet(GrowingArrayIdMapBuilder::of);
    }

    @Override
    public IdMapBuilder create(String id, Concurrency concurrency, Optional<Long> maxOriginalId, Optional<Long> nodeCount) {
        var idLowerCase = id.toLowerCase(Locale.US);
        if (idLowerCase.equals(ArrayIdMapBuilder.ID)) {
            return create(concurrency, maxOriginalId, nodeCount);
        }
        if (HighLimitIdMap.isHighLimitIdMap(idLowerCase)) {
            // We do not pass in the highest original id to the nested id map builder
            // since initializing a HighLimitIdMap is typically a situation where the
            // external ids may exceed the storage capabilities of the nested id map.
            // Instead, we _know_ that the highest original id for the nested id map
            // will be nodeCount - 1 as this is what the HighLimitIdMap guarantees.
            var maxIntermediateId = nodeCount.map(nc -> nc - 1);
            var innerBuilder = HighLimitIdMap.innerTypeId(idLowerCase)
                .map(innerId -> create(innerId, concurrency, maxIntermediateId, nodeCount))
                .orElseGet(() -> create(concurrency, maxIntermediateId, nodeCount));
            return HighLimitIdMapBuilder.of(concurrency, innerBuilder);        }
        return create(concurrency, maxOriginalId, nodeCount);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return ArrayIdMap.memoryEstimation();
    }
}
