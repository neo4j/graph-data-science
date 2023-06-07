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

import org.neo4j.gds.core.loading.ArrayIdMap;
import org.neo4j.gds.core.loading.ArrayIdMapBuilder;
import org.neo4j.gds.core.loading.GrowingArrayIdMapBuilder;
import org.neo4j.gds.core.loading.HighLimitIdMap;
import org.neo4j.gds.core.loading.HighLimitIdMapBuilder;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

import java.util.Locale;
import java.util.Optional;

public class OpenGdsIdMapBehavior implements IdMapBehavior {

    @Override
    public IdMapBuilder create(
        int concurrency,
        Optional<Long> maxOriginalId,
        Optional<Long> nodeCount
    ) {
        return nodeCount.or(() -> maxOriginalId.map(maxId -> maxId + 1))
            .map(capacity -> (IdMapBuilder) ArrayIdMapBuilder.of(capacity))
            .orElseGet(GrowingArrayIdMapBuilder::of);
    }

    @Override
    public IdMapBuilder create(String id, int concurrency, Optional<Long> maxOriginalId, Optional<Long> nodeCount) {
        var idLowerCase = id.toLowerCase(Locale.US);
        if (idLowerCase.equals(ArrayIdMapBuilder.ID)) {
            return create(concurrency, maxOriginalId, nodeCount);
        }
        if (HighLimitIdMap.isHighLimitIdMap(idLowerCase)) {
            var innerBuilder = HighLimitIdMap.innerTypeId(idLowerCase)
                .map(innerId -> create(innerId, concurrency, maxOriginalId, nodeCount))
                .orElseGet(() -> create(concurrency, maxOriginalId, nodeCount));
            return HighLimitIdMapBuilder.of(concurrency, innerBuilder);
        }
        return create(concurrency, maxOriginalId, nodeCount);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return ArrayIdMap.memoryEstimation();
    }
}
