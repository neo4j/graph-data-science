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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphCatalogConfigImpl;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.LazyIdMapBuilder;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GraphImporterThreadLocalBatchesTest {

    private static final User EMPTY_USER = new User("", false);

    private static final DatabaseId TEST_DATABASE_ID = DatabaseId.of("GraphImporterBatchSessionTest");

    private static final DatabaseInfo TEST_DATABASE_INFO = DatabaseInfo.create(
        TEST_DATABASE_ID,
        DatabaseInfo.DatabaseLocation.LOCAL
    );

    private GraphStoreCatalogService graphStoreCatalogService;

    @BeforeEach
    void setUp() {
        graphStoreCatalogService = new GraphStoreCatalogService();
    }

    @AfterEach
    void tearDown() {
        graphStoreCatalogService.removeAllLoadedGraphs(TEST_DATABASE_ID);
    }

    // readConcurrency 1 -> one slot per relationship type in the pooled builder
    private GraphImporter newImporter(String graphName) {
        return new GraphImporter(
            Log.noOpLog(),
            GraphCatalogConfigImpl.builder()
                .username("")
                .graphName(graphName)
                .readConcurrency(1)
                .build(),
            List.of(),
            List.of(),
            new LazyIdMapBuilder(new Concurrency(4), true, true, PropertyState.REMOTE),
            Capabilities.WriteMode.REMOTE,
            "",
            graphStoreCatalogService,
            ProgressTracker.NULL_TRACKER
        );
    }

    private static void update(
        GraphImporter importer,
        GraphImporter.ThreadLocalBatches session,
        long sourceNode,
        long targetNode,
        RelationshipType relationshipType
    ) {
        importer.update(
            session,
            sourceNode,
            targetNode,
            null,
            null,
            NodeLabelTokens.empty(),
            NodeLabelTokens.empty(),
            relationshipType,
            null
        );
    }

    private static void update(
        GraphImporter importer,
        long sourceNode,
        long targetNode,
        RelationshipType relationshipType
    ) {
        importer.update(
            sourceNode,
            targetNode,
            null,
            null,
            NodeLabelTokens.empty(),
            NodeLabelTokens.empty(),
            relationshipType,
            null
        );
    }

    @Test
    void shouldSupportMorselCycles() {
        var importer = newImporter("g");
        var session = importer.newThreadLocalBatches();

        // one batch per morsel: rows, release, rows, release, rows
        update(importer, session, 0, 1, RelationshipType.ALL_RELATIONSHIPS);
        update(importer, session, 1, 2, RelationshipType.ALL_RELATIONSHIPS);
        session.releaseBatches();

        update(importer, session, 2, 3, RelationshipType.ALL_RELATIONSHIPS);
        session.releaseBatches();

        update(importer, session, 3, 0, RelationshipType.ALL_RELATIONSHIPS);

        var result = importer.result(TEST_DATABASE_INFO, ProgressTimer.start(), true);

        assertThat(result.nodeCount()).isEqualTo(4);
        assertThat(result.relationshipCount()).isEqualTo(4);
    }

    @Test
    void shouldTrackBatchesPerRelationshipType() {
        var importer = newImporter("g");
        var session = importer.newThreadLocalBatches();

        var typeA = RelationshipType.of("A");
        var typeB = RelationshipType.of("B");

        update(importer, session, 0, 1, typeA);
        update(importer, session, 0, 1, typeB);
        session.releaseBatches();

        update(importer, session, 1, 2, typeA);
        update(importer, session, 1, 2, typeB);

        var result = importer.result(TEST_DATABASE_INFO, ProgressTimer.start(), true);

        assertThat(result.nodeCount()).isEqualTo(3);
        assertThat(result.relationshipCount()).isEqualTo(4);

        var graphStore = graphStoreCatalogService.getGraphStoreCatalogEntry(
            CatalogRequest.of(EMPTY_USER, TEST_DATABASE_ID),
            GraphName.parse("g")
        ).graphStore();

        assertThat(graphStore.relationshipTypes()).containsExactlyInAnyOrder(typeA, typeB);
        assertThat(graphStore.getGraph(typeA).relationshipCount()).isEqualTo(2);
        assertThat(graphStore.getGraph(typeB).relationshipCount()).isEqualTo(2);
    }

    @Test
    void shouldReleaseSlotToOtherSessions() {
        // pool size 1 per type: a second session can only make progress if the
        // first session released its claim
        var importer = newImporter("g");
        var sessionA = importer.newThreadLocalBatches();
        var sessionB = importer.newThreadLocalBatches();

        update(importer, sessionA, 0, 1, RelationshipType.ALL_RELATIONSHIPS);
        sessionA.releaseBatches();

        update(importer, sessionB, 1, 2, RelationshipType.ALL_RELATIONSHIPS);
        sessionB.releaseBatches();

        update(importer, sessionA, 2, 3, RelationshipType.ALL_RELATIONSHIPS);

        var result = importer.result(TEST_DATABASE_INFO, ProgressTimer.start(), true);

        assertThat(result.nodeCount()).isEqualTo(4);
        assertThat(result.relationshipCount()).isEqualTo(3);
    }

    @Test
    void shouldEvictClaimsBeforeBlockingOnANewType() {
        // The claim timeout is captured when the builder pools are created, so it
        // must be lowered before the first update creates them.
        var defaultTimeout = GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.get();
        GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.set(1);
        try {
            // readConcurrency 1 -> a single slot per relationship type
            var importer = newImporter("g");
            var typeA = RelationshipType.of("A");
            var typeB = RelationshipType.of("B");

            var sessionA = importer.newThreadLocalBatches();
            var sessionB = importer.newThreadLocalBatches();

            // session A holds the only B-slot
            update(importer, sessionA, 0, 1, typeB);
            // session B holds the only A-slot
            update(importer, sessionB, 1, 2, typeA);

            // session B needs a B-slot and would block on the pool held by
            // session A; blocking while holding its A-claim is what deadlocks,
            // so the A-claim must be evicted first
            assertThatThrownBy(() -> update(importer, sessionB, 2, 3, typeB))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Timed out");

            // the evicted A-slot is free again: another session claims it without blocking
            var sessionC = importer.newThreadLocalBatches();
            update(importer, sessionC, 3, 0, typeA);

            var result = importer.result(TEST_DATABASE_INFO, ProgressTimer.start(), true);

            assertThat(result.nodeCount()).isEqualTo(4);
            // the relationship of the failed session-B update is not counted
            assertThat(result.relationshipCount()).isEqualTo(3);
        } finally {
            GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.set(defaultTimeout);
        }
    }

    @Test
    void shouldMakeResultTerminalForSessions() {
        var importer = newImporter("g");
        // tracked, but never claims anything before result()
        var session = importer.newThreadLocalBatches();

        // legacy overload uses the importer-owned default session
        update(importer, 0, 1, RelationshipType.ALL_RELATIONSHIPS);
        update(importer, 1, 2, RelationshipType.ALL_RELATIONSHIPS);

        var result = importer.result(TEST_DATABASE_INFO, ProgressTimer.start(), true);
        assertThat(result.relationshipCount()).isEqualTo(2);

        // result() released every tracked session; further updates are rejected
        assertThatThrownBy(() -> update(importer, 0, 1, RelationshipType.ALL_RELATIONSHIPS))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Batch session is already closed");

        assertThatThrownBy(() -> update(importer, session, 0, 1, RelationshipType.ALL_RELATIONSHIPS))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Batch session is already closed");
    }
}
