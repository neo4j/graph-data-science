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
package org.neo4j.gds.core.loading.construction;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.gdl.ImmutableGraphProjectFromGdlConfig;
import org.neo4j.gds.utils.GdsFeatureToggles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RelationshipsBuilderBatchTest {

    private static RelationshipsBuilder pooledBuilder(Concurrency concurrency) {
        var gdlFactory = GdlFactory.builder().graphProjectConfig(
            ImmutableGraphProjectFromGdlConfig.builder()
                .gdlGraph("(a:A)-[:T]->(b:A), (c:A)-[:T]->(d:A)")
                .graphName("test")
                .build()
        ).build();
        var graphStore = gdlFactory.build();

        return GraphFactory.initRelationshipsBuilder()
            .nodes(graphStore.nodes())
            .relationshipType(RelationshipType.of("REL"))
            .orientation(Orientation.NATURAL)
            .concurrency(concurrency)
            .usePooledBuilderProvider(true)
            .build();
    }

    @Test
    void shouldReuseReleasedSlotInSubsequentBatch() {
        var relBuilder = pooledBuilder(new Concurrency(1));

        try (var batch = relBuilder.newBatch()) {
            assertThat(batch.addFromInternal(0, 1)).isTrue();
        }

        // the pool has exactly one slot; this only works if the batch above
        // actually returned it
        try (var batch = relBuilder.newBatch()) {
            assertThat(batch.addFromInternal(2, 3)).isTrue();
        }

        assertThat(relBuilder.build().count()).isEqualTo(2);
    }

    @Test
    void shouldKeepDataFromReleasedBatchesInBuild() {
        var relBuilder = pooledBuilder(new Concurrency(1));

        // several acquire/release cycles, i.e. one batch per morsel
        for (int i = 0; i < 3; i++) {
            try (var batch = relBuilder.newBatch()) {
                batch.addFromInternal(0, 1);
                batch.addFromInternal(2, 3);
            }
        }

        assertThat(relBuilder.build().count()).isEqualTo(6);
    }

    @Test
    void shouldFailWhenClaimingWhilePoolIsExhausted() {
        // the timeout is captured when the provider is created, i.e. when the
        // builder is built below
        var defaultTimeout = GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.get();
        GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.set(1);
        try {
            var relBuilder = pooledBuilder(new Concurrency(1));

            try (var ignored = relBuilder.newBatch()) {
                // holding the only slot
                assertThatThrownBy(relBuilder::newBatch)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Timed out after 1 seconds waiting for a pooled relationships builder slot");
            }
        } finally {
            GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.set(defaultTimeout);
        }
    }
}
