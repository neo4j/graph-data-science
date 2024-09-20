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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.compat.CompatExecutionContext;
import org.neo4j.gds.compat.PartitionedStoreScan;
import org.neo4j.gds.core.loading.RecordsBatchBuffer;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.Statement;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NodeLabelIndexScanTest extends BaseTest {

    private final Label aLabel = Label.label("A");
    private final Label bLabel = Label.label("B");

    private final Set<Long> aNodes = new HashSet<>();
    private final Set<Long> bNodes = new HashSet<>();

    @BeforeEach
    void setup() {
        var nc = 100_000;
        var r = new Random();

        try (var tx = db.beginTx()) {
            for (int i = 0; i < nc; i++) {
                if (r.nextBoolean()) {
                    aNodes.add(tx.createNode(aLabel).getId());
                } else {
                    bNodes.add(tx.createNode(bLabel).getId());
                }
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"A", "B"})
    void nodeLabelIndexScanTest(String label) {
        var tx = TestSupport.fullAccessTransaction(db);

        var expectedSet = label.equals("A")
            ? aNodes
            : bNodes;

        tx.accept((tx1, ktx) -> {
            var aToken = ktx.tokenRead().nodeLabel(label);
            var storeScan = PartitionedStoreScan.createScans(ktx, RecordsBatchBuffer.DEFAULT_BUFFER_SIZE, aToken)
                .get(0);

            try (
                var cursor = ktx.cursors().allocateNodeLabelIndexCursor(ktx.cursorContext())
            ) {
                try (
                    var ctx = new CompatExecutionContext() {

                        private final Statement stmt = ktx.acquireStatement();
                        private final ExecutionContext ctx1 = ktx.createExecutionContext();

                        @Override
                        public <C extends Cursor> boolean reservePartition(PartitionedScan<C> scan, C cursor1) {
                            return scan.reservePartition(cursor1, ctx1);
                        }

                        @Override
                        public void close() {
                            ctx1.complete();
                            ctx1.close();
                            stmt.close();
                        }
                    }
                ) {
                    var aNodesCount = 0;
                    while (storeScan.reserveBatch(cursor, ctx)) {
                        while (cursor.next()) {
                            assertThat(expectedSet.contains(cursor.nodeReference())).isTrue();
                            aNodesCount += 1;
                        }
                    }
                    assertThat(aNodesCount).isEqualTo(expectedSet.size());
                }
            }
        });
    }

}
