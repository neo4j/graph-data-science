/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.KernelTransactions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.ONLY;

public class NeighborhoodSimilarityTerminationTest extends ProcTestBase {

    private KernelTransactions kernelTransactions;

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        kernelTransactions = db.getDependencyResolver().resolveDependency(KernelTransactions.class, ONLY);
        registerProcedures(NeighborhoodSimilarityProc.class);
        createGraph(db, 5);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void shouldTerminateIfInterrupted() {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        concurrency: 1" +
                       "    }" +
                       ") YIELD node1, node2, similarity " +
                       "RETURN count(*) AS count";
        assertThrows(
            QueryExecutionException.class,
            () -> {
                try {
                    executeAndKill(query, row -> true);
                } catch (RuntimeException e) {
                    throw e.getCause();
                }
            }
        );
    }

    private void terminateTransaction(long txId) {
        kernelTransactions.activeTransactions()
            .stream()
            .filter(thx -> thx.lastTransactionIdWhenStarted() == txId)
            .forEach(thx -> thx.markForTermination(Status.Transaction.TransactionMarkedAsFailed));
    }

    private Map<String, Long> getQueryTransactionIds() {
        HashMap<String, Long> map = new HashMap<>();
        kernelTransactions.activeTransactions()
            .forEach(kth -> kth.executingQueries()
                .map(ExecutingQuery::queryText)
                .forEach(query -> map.put(query, kth.lastTransactionIdWhenStarted()))
            );
        return map;
    }

    private long findQueryTxId(String query) {
        return getQueryTransactionIds().getOrDefault(query, -1L);
    }

    private void executeAndKill(String query, Result.ResultVisitor<? extends Exception> visitor) {
        ArrayList<Runnable> runnables = new ArrayList<>();

        runnables.add(() -> {
            try {
                db.execute(query).accept(visitor);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        runnables.add(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            terminateTransaction(findQueryTxId(query));
        });

        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    static void createGraph(GraphDatabaseService db, int scaleFactor) {
        int itemCount = 1_000 * scaleFactor;
        Label itemLabel = Label.label("Item");
        int personCount = 10_000 * scaleFactor;
        Label personLabel = Label.label("Person");
        RelationshipType likesType = RelationshipType.withName("LIKES");

        List<Node> itemNodes = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                itemNodes.add(db.createNode(itemLabel));
            }
            for (int i = 0; i < personCount; i++) {
                Node person = db.createNode(personLabel);
                if (i % 6 == 0) {
                    int itemIndex = Math.floorDiv(i, 15);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    if (itemIndex > 0) person.createRelationshipTo(itemNodes.get(itemIndex - 1), likesType);
                }
                if (i % 5 == 0) {
                    int itemIndex = Math.floorDiv(i, 10);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    if (itemIndex + 1 < itemCount) person.createRelationshipTo(itemNodes.get(itemIndex + 1), likesType);
                }
                if (i % 4 == 0) {
                    int itemIndex = Math.floorDiv(i, 20);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    person.createRelationshipTo(itemNodes.get(itemIndex + 10), likesType);
                }
            }
            tx.success();
        }
    }
}
