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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodes.ComposedIdMap;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ShardedIdMapTest {

    private ShardedIdMap buildTranslator(long... originalIds) {
        var builder = ShardedLongLongMap.builder(new Concurrency(4));
        for (long id : originalIds) {
            builder.addNode(id);
        }
        return new ShardedIdMap(builder.build());
    }

    @Test
    void mapsExternalIdsToDenseMappedIds() {
        long high = Long.MAX_VALUE - 100;
        var translator = buildTranslator(high, high - 1, high - 2);

        assertThat(translator.nodeCount()).isEqualTo(3);
        assertThat(translator.typeId()).isEqualTo("sharded");

        // mapped ids are dense 0..n-1 and round-trip back to the external id
        for (long mappedId = 0; mappedId < translator.nodeCount(); mappedId++) {
            long original = translator.toOriginalNodeId(mappedId);
            assertThat(translator.toMappedNodeId(original)).isEqualTo(mappedId);
            assertThat(original).isGreaterThanOrEqualTo(high - 2);
        }
    }

    @Test
    void reportsHighestOriginalIdAndContainment() {
        long high = Long.MAX_VALUE - 100;
        var translator = buildTranslator(high, high - 1);

        assertThat(translator.highestOriginalId()).isEqualTo(high);
        assertThat(translator.containsOriginalId(high)).isTrue();
        assertThat(translator.containsOriginalId(42)).isFalse();
        assertThat(translator.toMappedNodeId(42)).isEqualTo(NodeTranslator.NOT_FOUND);
    }

    @Test
    void composedShardedIdMapIsItsOwnRoot() {
        var translator = buildTranslator(10, 20, 30);
        var labels = LabelInformationBuilders.allNodes().build(translator.nodeCount(), id -> id);
        var idMap = ComposedIdMap.of(translator, labels);

        idMap.forEachNode(mappedId -> {
            assertThat(idMap.toRootNodeId(mappedId)).isEqualTo(mappedId);
            return true;
        });
        assertThat(idMap.rootIdMap()).isSameAs(idMap);
    }

    @Test
    void withFilteredLabelsReturnsCommunityFilteredMap() {
        // Build a multi-label sharded id map via the lazy builder; in :core the community
        // behavior produces an ArrayIdMap-backed filtered translator.
        var builder = new LazyIdMapBuilderBuilder()
            .concurrency(new Concurrency(4))
            .hasLabelInformation(true)
            .hasProperties(false)
            .propertyState(PropertyState.PERSISTENT)
            .build();
        builder.addNode(1000, NodeLabelTokens.ofStrings("A"));
        builder.addNode(2000, NodeLabelTokens.ofStrings("B"));
        builder.addNode(3000, NodeLabelTokens.ofStrings("C"));
        var idMap = builder.build().idMap();

        var filtered = idMap.withFilteredLabels(List.of(NodeLabel.of("A")), new Concurrency(1));

        assertThat(filtered).isPresent();
        assertThat(filtered.get().nodeCount()).isEqualTo(1);
        // The filtered map is rooted in the dense sharded space; toRootNodeId(0) == mapped id of 1000.
        assertThat(filtered.get().toRootNodeId(0)).isEqualTo(idMap.toMappedNodeId(1000));
        assertThat(filtered.get().toOriginalNodeId(0)).isEqualTo(1000);
    }
}
