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

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongScatterSet;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.Label;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;

class NodeLabelIndexBasedScannerTest extends BaseTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMultipleNodeLabels(boolean allowPartitionedScan) {
        var nodeCount = 150_000;
        var prefetchSize = StoreScanner.DEFAULT_PREFETCH_SIZE;

        var labelABits = new Roaring64Bitmap();
        var labelBBits = new Roaring64Bitmap();

        var labelA = Label.label("A");
        var labelB = Label.label("B");

        runInTransaction(db, tx -> {
            for (int i = 0; i < nodeCount; i++) {
                if (i % 3 == 0) {
                    labelABits.add(tx.createNode(labelA).getId());
                } else if (i % 3 == 1) {
                    labelBBits.add(tx.createNode(labelB).getId());
                } else {
                    var id = tx.createNode(labelA, labelB).getId();
                    labelABits.add(id);
                    labelBBits.add(id);
                }
            }
        });

        try (var transactions = GraphDatabaseApiProxy.newKernelTransaction(db)) {
            var txContext = TransactionContext.of(db, transactions.tx());
            var ktx = transactions.ktx();

            var labelAToken = ktx.tokenRead().nodeLabel(labelA.name());
            var labelBToken = ktx.tokenRead().nodeLabel(labelB.name());

            var labelIds = new int[]{labelAToken, labelBToken};

            try (var scanner = new MultipleNodeLabelIndexBasedScanner(
                labelIds,
                prefetchSize,
                txContext,
                allowPartitionedScan
            );
                 var storeScanner = scanner.createCursor(ktx)) {

                var actualNodeCount = new MutableInt();

                var idList = new LongArrayList();
                var idSet = new LongScatterSet();


                while (storeScanner.scanBatch() && storeScanner.consumeBatch(nodeReference -> {
                    actualNodeCount.increment();

                    var neoId = nodeReference.nodeId();
                    idList.add(neoId);
                    idSet.add(neoId);
                    var labels = nodeReference.labels();

                    if (neoId % 3 == 0) {
                        assertThat(labels).contains(labelAToken);
                        assertThat(labels).doesNotContain(labelBToken);
                        assertThat(labelABits.contains(neoId)).isTrue();
                        assertThat(labelBBits.contains(neoId)).isFalse();
                    }
                    if (neoId % 3 == 1) {
                        assertThat(labels).doesNotContain(labelAToken);
                        assertThat(labels).contains(labelBToken);
                        assertThat(labelABits.contains(neoId)).isFalse();
                        assertThat(labelBBits.contains(neoId)).isTrue();
                    }
                    if (neoId % 3 == 2) {
                        assertThat(labels).contains(labelAToken);
                        assertThat(labels).contains(labelBToken);
                        assertThat(labelABits.contains(neoId)).isTrue();
                        assertThat(labelBBits.contains(neoId)).isTrue();
                    }
                    return true;
                })) {
                }

                assertThat(idList.size()).isEqualTo(idSet.size());
                assertThat(actualNodeCount.getValue()).isEqualTo(nodeCount);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false})
    void testBatchSizeAlignment(boolean allowPartitionedScan) {
        var prefetchSize = StoreScanner.DEFAULT_PREFETCH_SIZE;
        // batchSize = prefetch size * PAGE_SIZE / NodeRecord size
        var expectedBatchSize = 54_656;

        var label = Label.label("Node");
        long labelCount = 2 * expectedBatchSize;

        runInTransaction(db, tx -> {
            for (int i = 0; i < labelCount; i++) {
                tx.createNode(label);
            }
        });

        try (var transactions = GraphDatabaseApiProxy.newKernelTransaction(db)) {
            var txContext = TransactionContext.of(db, transactions.tx());
            var ktx = transactions.ktx();
            var labelToken = ktx.tokenRead().nodeLabel(label.name());

            try (
                var scanner = new NodeLabelIndexBasedScanner(
                    labelToken,
                    prefetchSize,
                    txContext,
                    allowPartitionedScan
                );
                var storeScanner = scanner.createCursor(ktx)
            ) {
                assertThat(scanner.batchSize()).isEqualTo(expectedBatchSize);

                var actualNodeCount = new MutableInt(0);
                var nodesPerPartition = new MutableInt(0);

                while (storeScanner.scanBatch() && storeScanner.consumeBatch(nodeReference -> {
                    nodesPerPartition.increment();
                    actualNodeCount.increment();
                    return true;
                })) {
                    assertThat(nodesPerPartition.intValue()).isLessThanOrEqualTo(expectedBatchSize);
                    nodesPerPartition.setValue(0);
                }

                assertThat(actualNodeCount.getValue()).isEqualTo(labelCount);
            }
        }
    }
}
