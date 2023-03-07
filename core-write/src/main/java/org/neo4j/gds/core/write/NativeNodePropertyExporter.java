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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.LazyBatchCollection;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.StatementApi;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.values.storable.Value;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public class NativeNodePropertyExporter extends StatementApi implements NodePropertyExporter {

    protected final TerminationFlag terminationFlag;
    protected final ExecutorService executorService;
    protected final ProgressTracker progressTracker;
    protected final int concurrency;
    protected final long nodeCount;
    protected final LongUnaryOperator toOriginalId;
    protected final LongAdder propertiesWritten;

    public static NodePropertyExporterBuilder<NativeNodePropertyExporter> builder(TransactionContext transactionContext, IdMap idMap, TerminationFlag terminationFlag) {
        return new NativeNodePropertiesExporterBuilder(transactionContext)
            .withIdMap(idMap)
            .withTerminationFlag(terminationFlag);
    }

    @SuppressWarnings("immutables:subtype")
    @ValueClass
    public interface ResolvedNodeProperty extends NodeProperty {
        int propertyToken();

        static ResolvedNodeProperty of(NodeProperty nodeProperty, int propertyToken) {
            return ImmutableResolvedNodeProperty.of(
                nodeProperty.propertyKey(),
                nodeProperty.properties(),
                propertyToken
            );
        }
    }

    public interface WriteConsumer {
        void accept(Write ops, long value) throws Exception;
    }

    protected NativeNodePropertyExporter(
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
        this.propertiesWritten = new LongAdder();
    }

    @Override
    public void write(String property, NodePropertyValues properties) {
        write(ImmutableNodeProperty.of(property, properties));
    }

    @Override
    public void write(NodeProperty nodeProperty) {
        write(List.of(nodeProperty));
    }

    private static NativeNodePropertyExporter.ResolvedNodeProperty resolveWith(NodeProperty property, int propertyToken) {
        if (propertyToken == -1) {
            throw new IllegalStateException("No write property token id is set.");
        }
        return NativeNodePropertyExporter.ResolvedNodeProperty.of(property, propertyToken);
    }

    @Override
    public void write(Collection<NodeProperty> nodeProperties) {
        var resolvedNodeProperties = nodeProperties.stream()
            .map(desc -> resolveWith(desc, getOrCreatePropertyToken(desc.propertyKey())))
            .collect(Collectors.toList());

        progressTracker.beginSubTask(nodeCount);
        try {
            if (ParallelUtil.canRunInParallel(executorService)) {
                writeParallel(resolvedNodeProperties);
            } else {
                writeSequential(resolvedNodeProperties);
            }
        } finally {
            progressTracker.endSubTask();
        }
    }

    @Override
    public long propertiesWritten() {
        return propertiesWritten.longValue();
    }

    private void writeSequential(Iterable<ResolvedNodeProperty> nodeProperties) {
        writeSequential((ops, nodeId) -> doWrite(nodeProperties, ops, nodeId));
    }

    private void writeParallel(Iterable<ResolvedNodeProperty> nodeProperties) {
        writeParallel((ops, offset) -> doWrite(nodeProperties, ops, offset));
    }

    private void doWrite(Iterable<ResolvedNodeProperty> nodeProperties, Write ops, long nodeId) throws Exception {
        for (ResolvedNodeProperty nodeProperty : nodeProperties) {
            int propertyId = nodeProperty.propertyToken();
            final Value prop = nodeProperty.properties().value(nodeId);
            if (prop != null) {
                ops.nodeSetProperty(
                    toOriginalId.applyAsLong(nodeId),
                    propertyId,
                    prop
                );
                propertiesWritten.increment();
            }
        }
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
