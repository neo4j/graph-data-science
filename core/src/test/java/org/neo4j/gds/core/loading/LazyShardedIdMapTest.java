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
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;

import static org.assertj.core.api.Assertions.assertThat;

class LazyShardedIdMapTest {

    @Test
    void lazyBuilderProducesShardedIdMap() {
        var builder = new LazyIdMapBuilderBuilder()
            .concurrency(new Concurrency(4))
            .hasLabelInformation(true)
            .hasProperties(false)
            .propertyState(PropertyState.PERSISTENT)
            .build();

        builder.addNode(1000, NodeLabelTokens.ofStrings("A"));
        builder.addNode(2000, NodeLabelTokens.ofStrings("B"));
        builder.addNode(3000, NodeLabelTokens.ofStrings("C"));

        var result = builder.build();
        var idMap = result.idMap();

        assertThat(idMap).isInstanceOf(ShardedIdMap.class);
        assertThat(idMap.typeId()).isEqualTo("sharded");
        assertThat(idMap.nodeCount()).isEqualTo(3);
        assertThat(idMap.toOriginalNodeId(idMap.toMappedNodeId(2000))).isEqualTo(2000);
        // intermediate id map collapses to identity
        idMap.forEachNode(mappedId -> {
            assertThat(result.intermediateIdMap().toMappedNodeId(mappedId)).isEqualTo(mappedId);
            return true;
        });
    }

    @Test
    void unmappedExternalIdReturnsNotFound() {
        var builder = new LazyIdMapBuilderBuilder()
            .concurrency(new Concurrency(1))
            .hasLabelInformation(false)
            .hasProperties(false)
            .propertyState(PropertyState.PERSISTENT)
            .build();
        builder.addNode(5, NodeLabelTokens.empty());

        var idMap = builder.build().idMap();
        assertThat(idMap.toMappedNodeId(1337)).isEqualTo(org.neo4j.gds.api.IdMap.NOT_FOUND);
        assertThat(idMap.containsOriginalId(1337)).isFalse();
    }
}
