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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class ShardedIdMapBuilderTest {

    @Test
    void buildsShardedIdMapFromBatches() {
        var concurrency = new Concurrency(1);
        long offset = Long.MAX_VALUE - 100;
        long[] nodes = LongStream.range(0, 10).map(i -> offset - i).toArray();

        var builder = ShardedIdMapBuilder.of(concurrency);
        builder.allocate(nodes.length).insert(nodes.clone());
        var idMap = builder.build(LabelInformationBuilders.allNodes(), nodes.length - 1, concurrency);

        assertThat(idMap).isInstanceOf(ShardedIdMap.class);
        assertThat(idMap.typeId()).isEqualTo("sharded");
        assertThat(idMap.nodeCount()).isEqualTo(10);
        assertThat(idMap.toMappedNodeId(offset)).isGreaterThanOrEqualTo(0);
        assertThat(idMap.toOriginalNodeId(idMap.toMappedNodeId(offset))).isEqualTo(offset);
    }

    @Test
    void recognizesShardedAndLegacyHighLimitTypes() {
        assertThat(ShardedIdMapBuilder.isShardedIdMapType("sharded")).isTrue();
        assertThat(ShardedIdMapBuilder.isShardedIdMapType("highlimit")).isTrue();
        assertThat(ShardedIdMapBuilder.isShardedIdMapType("highlimit-array")).isTrue();
        assertThat(ShardedIdMapBuilder.isShardedIdMapType("highlimit-bit")).isTrue();
        assertThat(ShardedIdMapBuilder.isShardedIdMapType("array")).isFalse();
        assertThat(ShardedIdMapBuilder.isShardedIdMapType("bit")).isFalse();
    }
}
