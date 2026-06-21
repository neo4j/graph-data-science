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
package org.neo4j.gds.core.loading.nodeproperties;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.ShardedIdMapBuilder;
import org.neo4j.gds.values.primitive.PrimitiveValues;

import static org.assertj.core.api.Assertions.assertThat;

class ShardedIdMapPropertiesTest {

    @Test
    void resolvesPropertiesByDenseMappedIdForShardedIdMap() {
        var concurrency = new Concurrency(1);
        long offset = Long.MAX_VALUE - 100;
        long[] nodes = {offset, offset - 1, offset - 2};

        var idMapBuilder = ShardedIdMapBuilder.of(concurrency);
        var allocator = idMapBuilder.allocate(nodes.length);
        // overrideIds=true rewrites the array in place with the dense ids
        var batch = nodes.clone();
        allocator.insert(batch);

        var propsBuilder = NodePropertiesFromStoreBuilder.of(DefaultValue.forLong(), concurrency);
        // properties are keyed by the dense id produced for each node (batch now holds them)
        for (int i = 0; i < batch.length; i++) {
            propsBuilder.set(batch[i], PrimitiveValues.longValue(100L + i));
        }

        var idMap = idMapBuilder.build(LabelInformationBuilders.allNodes(), nodes.length - 1, concurrency);
        var values = propsBuilder.build(idMap);

        // node with external id `offset` must carry value 100, etc., addressed by mapped id
        long mapped0 = idMap.toMappedNodeId(offset);
        assertThat(values.longValue(mapped0)).isEqualTo(100L);
        assertThat(values.longValue(idMap.toMappedNodeId(offset - 1))).isEqualTo(101L);
        assertThat(values.longValue(idMap.toMappedNodeId(offset - 2))).isEqualTo(102L);
    }
}
