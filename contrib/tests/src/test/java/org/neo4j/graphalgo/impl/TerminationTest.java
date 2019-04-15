/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TerminateProcedure;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 *
 * @author mknblch
 */
public class TerminationTest {

    private static GraphDatabaseAPI api;

    private static KernelTransactions kernelTransactions;

    @BeforeClass
    public static void setup() throws KernelException {

        api = TestDatabaseCreator.createTestDatabase();

        final Procedures procedures = api.getDependencyResolver()
                .resolveDependency(Procedures.class);

        procedures.registerProcedure(TerminateProcedure.class);

        kernelTransactions = api.getDependencyResolver().resolveDependency(KernelTransactions.class);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (api != null) api.shutdown();
    }

    // terminate a transaction by its id
    private void terminateTransaction(long txId) {
        kernelTransactions.activeTransactions()
                .stream()
                .filter(thx -> thx.lastTransactionIdWhenStarted() == txId)
                .forEach(thx -> {
                    System.out.println("terminating transaction " + txId);
                    thx.markForTermination(Status.Transaction.TransactionMarkedAsFailed);
                });
    }

    // get map of currently running queries and its IDs
    private Map<String, Long> getQueryTransactionIds() {
        final HashMap<String, Long> map = new HashMap<>();
        kernelTransactions.activeTransactions()
                .forEach(kth -> {
                    final String query = kth.executingQueries()
                            .map(q -> q.queryText())
                            .collect(Collectors.joining(", "));
                    map.put(query, kth.lastTransactionIdWhenStarted());
                });
        return map;
    }

    // find tx id to query
    private long findQueryTxId(String query) {
        return getQueryTransactionIds().getOrDefault(query, -1L);
    }

    // execute query as usual but also submits a termination thread which kills the tx after a timeout
    private void executeAndKill(String query, long killTimeout, Result.ResultVisitor<? extends Exception> visitor) {
        final ArrayList<Runnable> runnables = new ArrayList<>();

        // add query runnable
        runnables.add(() -> {
            try {
                api.execute(query).accept(visitor);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        // add killer runnable
        runnables.add(() -> {
            try {
                Thread.sleep(killTimeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            terminateTransaction(findQueryTxId(query));
        });

        // submit
        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    @Test(expected = TransactionFailureException.class)
    public void test() throws Throwable {

        try {
            executeAndKill("CALL test.testProc()", 2000L, row -> true);
        } catch (RuntimeException e) {
            throw e.getCause();
        }
    }

}
