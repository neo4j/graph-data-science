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
package org.neo4j.graphalgo.core.write;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public final class NodePropertyStoreExporter extends NodePropertyExporter {

    private final Optional<Map<NodeLabel, BitSet>> maybeLabelInformation;
    private final Map<NodeLabel, GraphStore.NodePropertyStore> graphStoreNodeProperties;

    public interface WriteConsumer {
        void accept(Write ops, long value) throws Exception;
    }

    NodePropertyStoreExporter(
        GraphDatabaseAPI db,
        long nodeCount,
        LongUnaryOperator toOriginalId,
        TerminationFlag terminationFlag,
        Map<NodeLabel, GraphStore.NodePropertyStore> graphStoreNodeProperties,
        Optional<Map<NodeLabel, BitSet>> maybeLabelInformation,
        ProgressLogger log,
        int concurrency,
        ExecutorService executorService
    ) {
        super(db, nodeCount, toOriginalId, terminationFlag, log, concurrency, executorService);
        this.graphStoreNodeProperties = graphStoreNodeProperties;
        this.maybeLabelInformation = maybeLabelInformation;
    }

    protected void writeSequential(List<NodePropertyExporter.ResolvedNodeProperty> nodeProperties) {
        if (maybeLabelInformation.isEmpty()) {
            super.writeSequential((ops, nodeId) -> doWrite(nodeProperties, ops, nodeId));
        } else {
            maybeLabelInformation.get().forEach((nodeLabel, bitSet) -> {
                List<ResolvedNodeProperty> filteredProperties = filterNodePropertiesForLabel(nodeProperties, nodeLabel);
                writeSequential(bitSet, (ops, nodeId) -> doWrite(filteredProperties, ops, nodeId));
            });
        }
    }

    protected void writeParallel(List<NodePropertyExporter.ResolvedNodeProperty> nodeProperties) {
        if (maybeLabelInformation.isEmpty()) {
            super.writeParallel((ops, offset) -> doWrite(nodeProperties, ops, offset));
        } else {
            maybeLabelInformation.get().forEach(((nodeLabel, bitSet) -> {
                List<ResolvedNodeProperty> filteredProperties = filterNodePropertiesForLabel(nodeProperties, nodeLabel);
                writeParallel(bitSet, (ops, nodeId) -> doWrite(filteredProperties, ops, nodeId));
            }));
        }
    }

    protected void writeSequential(BitSet bitset, WriteConsumer writer) {
        acceptInTransaction(stmt -> {
            terminationFlag.assertRunning();
            long progress = 0L;
            Write ops = stmt.dataWrite();
            long nodeId = BitSetIterator.NO_MORE;
            while((nodeId = bitset.nextSetBit(nodeId+1)) != -1) {
                writer.accept(ops, nodeId);
                ++progress;
                if (progress % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                    progressLogger.logProgress(progress, nodeCount);
                    terminationFlag.assertRunning();
                }
            }
            progressLogger.logProgress(
                nodeCount,
                nodeCount
            );
        });
    }

    protected void writeParallel(BitSet bitSet, WriteConsumer writer) {
        final long batchSize = ParallelUtil.adjustedBatchSize(
            nodeCount,
            concurrency,
            MIN_BATCH_SIZE,
            MAX_BATCH_SIZE
        );
        final AtomicLong progress = new AtomicLong(0L);
        final Collection<Runnable> runnables = LazyBatchCollection.of(
            nodeCount,
            batchSize,
            (start, len) -> () -> {
                acceptInTransaction(stmt -> {
                    terminationFlag.assertRunning();
                    long end = start + len;
                    Write ops = stmt.dataWrite();
                    for (long currentNode = start; currentNode < end; currentNode++) {
                        if (bitSet.get(currentNode)) {
                            writer.accept(ops, currentNode);

                            // Only log every 10_000 written nodes
                            if ((currentNode - start) % TerminationFlag.RUN_CHECK_NODE_COUNT == 0) {
                                long currentProgress = progress.addAndGet(TerminationFlag.RUN_CHECK_NODE_COUNT);
                                progressLogger.logProgress(
                                    currentProgress,
                                    nodeCount
                                );
                                terminationFlag.assertRunning();
                            }
                        }
                    }

                    // log progress for the last batch of written nodes
                    progressLogger.logProgress(
                        progress.addAndGet((end - start + 1) % TerminationFlag.RUN_CHECK_NODE_COUNT),
                        nodeCount
                    );
                });
            }
        );
        ParallelUtil.runWithConcurrency(
            concurrency,
            runnables,
            Integer.MAX_VALUE,
            10L,
            TimeUnit.MICROSECONDS,
            terminationFlag,
            executorService
        );
    }

    private List<ResolvedNodeProperty> filterNodePropertiesForLabel(List<ResolvedNodeProperty> nodeProperties, NodeLabel nodeLabel) {
        return nodeProperties
            .stream()
            .filter(prop -> graphStoreNodeProperties.containsKey(nodeLabel) && graphStoreNodeProperties
                .get(nodeLabel)
                .containsKey(prop.propertyKey()))
            .collect(Collectors.toList());
    }

}

