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
package org.neo4j.gds.impl.walking;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.msbfs.MSBFSConstants;
import org.neo4j.gds.msbfs.MultiSourceBFSRunnable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class CollapsePathTaskSupplier implements Supplier<Runnable> {
    // path template index - which stack of graphs are we working on?
    private final AtomicInteger globalSharedPathTemplateIndex = new AtomicInteger(0);

    // node index
    private final List<AtomicLong> globalSharedBatchOffsets;

    private final List<Graph[]> pathTemplates;
    private final long nodeCount;
    private final List<TraversalConsumer> bfsConsumers;
    private final boolean allowSelfLoops;

    static Supplier<Runnable> create(
        RelationshipsBuilder relationshipsBuilder,
        boolean allowSelfLoops,
        List<Graph[]> pathTemplates,
        long nodeCount
    ) {
        List<TraversalConsumer> traversalConsumers = pathTemplates.stream()
            .map(
                pathTemplate -> allowSelfLoops
                    ? new TraversalConsumer(relationshipsBuilder, pathTemplate.length)
                    : new NoLoopTraversalConsumer(relationshipsBuilder, pathTemplate.length)
            )
            .collect(Collectors.toList());

        List<AtomicLong> globalSharedBatchOffsets = pathTemplates.stream()
            .map(pathTemplate -> new AtomicLong(0))
            .collect(Collectors.toList());

        return new CollapsePathTaskSupplier(
            globalSharedBatchOffsets,
            pathTemplates,
            nodeCount,
            traversalConsumers,
            allowSelfLoops
        );
    }

    private CollapsePathTaskSupplier(
        List<AtomicLong> globalSharedBatchOffsets,
        List<Graph[]> pathTemplates,
        long nodeCount,
        List<TraversalConsumer> bfsConsumers,
        boolean allowSelfLoops
    ) {
        this.globalSharedBatchOffsets = globalSharedBatchOffsets;
        this.pathTemplates = pathTemplates;
        this.nodeCount = nodeCount;
        this.bfsConsumers = bfsConsumers;
        this.allowSelfLoops = allowSelfLoops;
    }

    /**
     * A task will process (up to) omega (64) nodes in a chunk.
     */
    @Override
    public Runnable get() {
        return () -> {
            while (true) {
                // record which path template we are working on
                int pathTemplateIndex = globalSharedPathTemplateIndex.get();

                if (pathTemplateIndex >= pathTemplates.size()) return; // we have exhausted all!

                // path template == list of relationship types, but encoded as list of single relationship type graphs
                RelationshipIterator[] localPathTemplate = getPathTemplate(pathTemplateIndex);

                var offset = -1L;
                var startNodes = new long[MSBFSConstants.OMEGA];
                AtomicLong offsetHolder = globalSharedBatchOffsets.get(pathTemplateIndex);

                // remember that this loop happens on many thread concurrently, hence the shared offset counter
                while ((offset = offsetHolder.getAndAdd(MSBFSConstants.OMEGA)) < nodeCount) {
                    // at the end of the array we might not have a whole omega-sized chunk left, so we resize
                    if (offset + MSBFSConstants.OMEGA >= nodeCount) {
                        var numberOfNodesRemaining = (int) (nodeCount - offset);
                        startNodes = new long[numberOfNodesRemaining];
                    }

                    for (int i = 0; i < startNodes.length; i++) {
                        startNodes[i] = offset + i;
                    }

                    var msbfsTask = createMSBFSTask(localPathTemplate, startNodes, pathTemplateIndex);

                    msbfsTask.run();
                }

                // once we exhaust one stack of layers, we move to next one
                globalSharedPathTemplateIndex.compareAndSet(pathTemplateIndex, pathTemplateIndex + 1);
            }
        };
    }

    /**
     * Make concurrent copies of the stack of graphs/ layers so that we may run stuff concurrently
     */
    @NotNull
    private RelationshipIterator[] getPathTemplate(int pathTemplateIndex) {
        var pathTemplate = pathTemplates.get(pathTemplateIndex);

        var localPathTemplate = new RelationshipIterator[pathTemplate.length];

        for (int i = 0; i < pathTemplate.length; i++) {
            localPathTemplate[i] = pathTemplate[i].concurrentCopy();
        }

        return localPathTemplate;
    }

    private MultiSourceBFSRunnable createMSBFSTask(
        RelationshipIterator[] localPathTemplate,
        long[] startNodes,
        int pathTemplateIndex
    ) {
        var executionStrategy = new TraversalToEdgeMSBFSStrategy(
            localPathTemplate,
            bfsConsumers.get(pathTemplateIndex)
        );

        return MultiSourceBFSRunnable.createWithoutSeensNext(
            nodeCount,
            null,
            executionStrategy,
            allowSelfLoops,
            startNodes
        );
    }
}
