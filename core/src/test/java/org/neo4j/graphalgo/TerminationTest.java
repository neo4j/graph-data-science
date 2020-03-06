/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.KernelTransactionsProxy;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.kernel.impl.api.KernelTransactions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.compat.Transactions.transactionFailureException;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 */
class TerminationTest extends BaseProcTest {

    public static final String QUERY = "CALL test.testProc()";

    private KernelTransactions kernelTransactions;

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(TerminateProcedure.class);
        kernelTransactions = resolveDependency(KernelTransactions.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    // terminate a transaction by its id
    private void terminateTransaction(long txId) {
        kernelTransactions.activeTransactions()
            .stream()
            .filter(thx -> KernelTransactionsProxy.lastTransactionIdWhenStarted(thx) == txId)
            .forEach(thx -> {
                System.out.println("terminating transaction " + txId);
                KernelTransactionsProxy.markForTermination(thx);
            });
    }

    // get map of currently running queries and its IDs
    private Map<String, Long> getQueryTransactionIds() {
        Map<String, Long> map = new HashMap<>();
        kernelTransactions.activeTransactions().forEach(kth -> {
            String query = KernelTransactionsProxy.executingQueryTexts(kth, ", ");
            map.put(query, KernelTransactionsProxy.lastTransactionIdWhenStarted(kth));
        });
        return map;
    }

    // find tx id to query
    private long findQueryTxId() {
        return getQueryTransactionIds().getOrDefault(TerminationTest.QUERY, -1L);
    }

    // execute query as usual but also submits a termination thread which kills the tx after a timeout
    private void executeAndKill() {
        final ArrayList<Runnable> runnables = new ArrayList<>();

        // add query runnable
        runnables.add(() -> {
            try {
                runQuery(TerminationTest.QUERY);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        // add killer runnable
        runnables.add(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            terminateTransaction(findQueryTxId());
        });

        // submit
        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    @Test
    void test() {
        assertThrows(
            transactionFailureException(),
                () -> {
                    try {
                        executeAndKill();
                    } catch (RuntimeException e) {
                        throw e.getCause();
                    }
                }
        );
    }

}
