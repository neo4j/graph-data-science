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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;

import java.util.Locale;

public final class ShardedIdMapBuilder implements IdMapBuilder {

    public static final String ID = "sharded";

    // Legacy serialized graphs were written with a "highlimit-<inner>" type id.
    // They are reconstructed as a ShardedIdMap on restore.
    private static final String LEGACY_HIGH_LIMIT_PREFIX = "highlimit";

    private final ShardedLongLongMap.BatchedBuilder builder;

    public static ShardedIdMapBuilder of(Concurrency concurrency) {
        return new ShardedIdMapBuilder(concurrency);
    }

    private ShardedIdMapBuilder(Concurrency concurrency) {
        // overrideIds = true: the input batch is overwritten in place with the generated
        // dense ids so downstream label and property processing keys off the dense id.
        this.builder = ShardedLongLongMap.batchedBuilder(concurrency, true);
    }

    @Override
    public IdMapAllocator allocate(int batchLength) {
        return this.builder.prepareBatch(batchLength);
    }

    @Override
    public IdMap build(LabelInformation.Builder labelInformationBuilder, long highestNodeId, Concurrency concurrency) {
        var idMap = this.builder.build();
        // Dense ids: the label keys (already dense intermediate ids) map to themselves.
        var labelInformation = labelInformationBuilder.build(idMap.size(), id -> id);
        return new ShardedIdMap(idMap, labelInformation);
    }

    public static boolean isShardedIdMapType(String typeId) {
        var lowerCase = typeId.toLowerCase(Locale.US);
        return lowerCase.equals(ID) || lowerCase.startsWith(LEGACY_HIGH_LIMIT_PREFIX);
    }
}
