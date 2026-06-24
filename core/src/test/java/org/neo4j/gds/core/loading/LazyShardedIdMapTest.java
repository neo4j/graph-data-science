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
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class LazyShardedIdMapTest {

    @Test
    void labelInformationIsAttributedToCorrectNodesUnderConcurrency() throws Exception {
        var builder = new LazyIdMapBuilderBuilder()
            .concurrency(new Concurrency(4))
            .hasLabelInformation(true)
            .hasProperties(false)
            .propertyState(PropertyState.PERSISTENT)
            .build();

        int nodeCount = 20_000;
        var even = NodeLabel.of("Even");
        var odd = NodeLabel.of("Odd");

        // Add nodes concurrently so that the inner id-map builder's allocation order
        // diverges from the intermediate id order. Each original id carries a label
        // determined by its parity, so any mis-attribution is detectable per node.
        var executor = Executors.newFixedThreadPool(4);
        try {
            var futures = new ArrayList<Future<?>>();
            int chunk = nodeCount / 4;
            for (int t = 0; t < 4; t++) {
                long start = (long) t * chunk;
                long end = (t == 3) ? nodeCount : start + chunk;
                futures.add(executor.submit(() -> {
                    for (long original = start; original < end; original++) {
                        var label = (original % 2 == 0) ? "Even" : "Odd";
                        builder.addNode(original, NodeLabelTokens.ofStrings(label));
                    }
                }));
            }
            for (var future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }

        var idMap = builder.build().idMap();
        assertThat(idMap.nodeCount()).isEqualTo(nodeCount);

        for (long original = 0; original < nodeCount; original++) {
            long mapped = idMap.toMappedNodeId(original);
            var expected = (original % 2 == 0) ? even : odd;
            var unexpected = (original % 2 == 0) ? odd : even;
            assertThat(idMap.hasLabel(mapped, expected))
                .as("original node %d (mapped %d) should have label %s", original, mapped, expected.name())
                .isTrue();
            assertThat(idMap.hasLabel(mapped, unexpected))
                .as("original node %d (mapped %d) should not have label %s", original, mapped, unexpected.name())
                .isFalse();
        }
    }

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

        assertThat(idMap).isInstanceOf(ComposedIdMap.class);
        assertThat(((ComposedIdMap) idMap).nodeTranslator()).isInstanceOf(ShardedIdMap.class);
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
        assertThat(idMap.toMappedNodeId(1337)).isEqualTo(IdMap.NOT_FOUND);
        assertThat(idMap.containsOriginalId(1337)).isFalse();
    }
}
