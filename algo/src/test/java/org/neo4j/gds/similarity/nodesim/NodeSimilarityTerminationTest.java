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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeSimilarityTerminationTest extends BaseTest {

    @Timeout(value = 10)
    @Test
    void shouldTerminate() {
        HugeGraph graph = RandomGraphGenerator.builder()
            .nodeCount(10)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .build()
            .generate();

        NodeSimilarity nodeSimilarity = NodeSimilarity.create(
            graph,
            NodeSimilarityTest.configBuilder().concurrency(1).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        assertAlgorithmTermination(
            db,
            nodeSimilarity,
            nhs -> nodeSimilarity.computeToStream(),
            200
        );
    }

    /**
     * This method assumes that the given algorithm calls {@link org.neo4j.gds.Algorithm#assertRunning()} at least once.
     * When called, the algorithm will sleep for {@code sleepMillis} milliseconds before it checks the transaction state.
     * A second thread will terminate the transaction during the sleep interval.
     */
    static void assertAlgorithmTermination(
        GraphDatabaseService db,
        Algorithm<?> algorithm,
        Consumer<Algorithm<?>> algoConsumer,
        long sleepMillis
    ) {
        assert sleepMillis >= 100 && sleepMillis <= 10_000;

        try (var timeoutTx = db.beginTx(10, TimeUnit.SECONDS)) {
            var kernelTx = ((InternalTransaction) timeoutTx).kernelTransaction();

            algorithm.setTerminationFlag(new TestTerminationFlag(kernelTx, sleepMillis));

            Runnable algorithmThread = () -> {
                try {
                    algoConsumer.accept(algorithm);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            Runnable interruptingThread = () -> {
                try {
                    Thread.sleep(sleepMillis / 2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                kernelTx.markForTermination(Status.Transaction.TransactionMarkedAsFailed);
            };

            assertThrows(
                TransactionTerminatedException.class,
                () -> {
                    try {
                        ParallelUtil.run(Arrays.asList(algorithmThread, interruptingThread), Pools.DEFAULT);
                    } catch (RuntimeException e) {
                        throw e.getCause();
                    }
                }
            );
        }
    }

    static class TestTerminationFlag implements TerminationFlag {

        private final KernelTransaction transaction;
        private final long sleepMillis;

        TestTerminationFlag(KernelTransaction transaction, long sleepMillis) {
            this.transaction = transaction;
            this.sleepMillis = sleepMillis;
        }

        @Override
        public boolean running() {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {}
            return !transaction.getReasonIfTerminated().isPresent() && transaction.isOpen();
        }
    }
}
