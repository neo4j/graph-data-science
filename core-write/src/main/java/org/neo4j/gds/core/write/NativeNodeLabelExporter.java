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
package org.neo4j.gds.core.write;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.LazyBatchCollection;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.StatementApi;
import org.neo4j.internal.kernel.api.Write;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongUnaryOperator;

public class NativeNodeLabelExporter extends StatementApi implements NodeLabelExporter {

    private final TerminationFlag terminationFlag;
    private final ExecutorService executorService;
    private final ProgressTracker progressTracker;
    private final int concurrency;
    private final long nodeCount;
    private final LongUnaryOperator toOriginalId;
    private final LongAdder nodeLabelsWritten;

    public interface WriteConsumer {
        void accept(Write ops, long value) throws Exception;
    }

    public static NodeLabelExporterBuilder<NativeNodeLabelExporter> builder(TransactionContext transactionContext, IdMap idMap, TerminationFlag terminationFlag) {
        return new NativeNodeLabelExporterBuilder(transactionContext)
            .withIdMap(idMap)
            .withTerminationFlag(terminationFlag);
    }

    NativeNodeLabelExporter(
        TransactionContext tx,
        long nodeCount,
        LongUnaryOperator toOriginalId,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        int concurrency,
        ExecutorService executorService
    ) {
        super(tx);
        this.nodeCount = nodeCount;
        this.toOriginalId = toOriginalId;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.nodeLabelsWritten = new LongAdder();
    }

    @Override
    public void write(String nodeLabel) {

        int nodeLabelToken = getOrCreateNodeLabelToken(nodeLabel);
        progressTracker.beginSubTask(nodeCount);
        try {
            if (concurrency > 1 && ParallelUtil.canRunInParallel(executorService)) {
                writeParallel(nodeLabelToken);
            } else {
                writeSequential(nodeLabelToken);
            }
        } finally {
            progressTracker.endSubTask();
        }
    }

    @Override
    public long nodeLabelsWritten() {
        return nodeLabelsWritten.longValue();
    }

    private void writeSequential(int nodeLabelToken) {
        writeSequential((ops, nodeId) -> doWrite(ops, nodeId, nodeLabelToken));
    }

    private void writeParallel(int nodeLabelToken) {
        writeParallel((ops, offset) -> doWrite(ops, offset, nodeLabelToken));
    }

    private void doWrite(Write ops, long nodeId, int nodeLabelToken) throws Exception {
            ops.nodeAddLabel(
                toOriginalId.applyAsLong(nodeId),
                nodeLabelToken
            );
            nodeLabelsWritten.increment();
    }

    private void writeSequential(WriteConsumer writer) {
        acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            long progress = 0L;
            Write ops = stmt.dataWrite();
            for (long i = 0L; i < nodeCount; i++) {
                writer.accept(ops, i);
                progressTracker.logProgress();
                if (++progress % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                    terminationFlag.assertRunning();
                }
            }
        });
    }

    private void writeParallel(WriteConsumer writer) {
        final long batchSize = ParallelUtil.adjustedBatchSize(
            nodeCount,
            concurrency,
            MIN_BATCH_SIZE,
            MAX_BATCH_SIZE
        );
        final Collection<Runnable> runnables = LazyBatchCollection.of(
            nodeCount,
            batchSize,
            (start, len) -> () -> {
                acceptInTransaction(stmt -> {
                    terminationFlag.assertRunning();
                    long end = start + len;
                    Write ops = stmt.dataWrite();
                    for (long currentNode = start; currentNode < end; currentNode++) {
                        writer.accept(ops, currentNode);
                        progressTracker.logProgress();

                        if ((currentNode - start) % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                            terminationFlag.assertRunning();
                        }
                    }
                });
            }
        );
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(runnables)
            .maxWaitRetries(Integer.MAX_VALUE)
            .waitTime(10L, TimeUnit.MICROSECONDS)
            .terminationFlag(terminationFlag)
            .executor(executorService)
            .mayInterruptIfRunning(false)
            .run();
    }
}
