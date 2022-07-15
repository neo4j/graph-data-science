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
import org.neo4j.gds.msbfs.BfsConsumer;
import org.neo4j.gds.msbfs.MSBFSConstants;
import org.neo4j.gds.msbfs.MultiSourceBFSRunnable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class CollapsePathTaskSupplier implements Supplier<Runnable> {
    private final AtomicLong globalSharedBatchOffset = new AtomicLong(0);

    private final RelationshipIterator[] relationshipIterators;
    private final long nodeCount;
    private final BfsConsumer bfsConsumer;
    private final boolean allowSelfLoops;

    static Supplier<Runnable> create(
        RelationshipsBuilder relationshipsBuilder,
        boolean allowSelfLoops,
        Graph[] graphs,
        long nodeCount
    ) {
        var traversalConsumer = allowSelfLoops
            ? new TraversalConsumer(relationshipsBuilder, graphs.length)
            : new NoLoopTraversalConsumer(relationshipsBuilder, graphs.length);

        return new CollapsePathTaskSupplier(
            graphs,
            nodeCount,
            traversalConsumer,
            allowSelfLoops
        );
    }

    private CollapsePathTaskSupplier(
        RelationshipIterator[] relationshipIterators,
        long nodeCount,
        BfsConsumer bfsConsumer,
        boolean allowSelfLoops
    ) {
        this.relationshipIterators = relationshipIterators;
        this.nodeCount = nodeCount;
        this.bfsConsumer = bfsConsumer;
        this.allowSelfLoops = allowSelfLoops;
    }

    /**
     * A task will process (up to) omega (64) nodes in a chunk.
     */
    @Override
    public Runnable get() {
        return () -> {
            RelationshipIterator[] localRelationshipIterators = getRelationshipIterators();

            var offset = -1L;
            var startNodes = new long[MSBFSConstants.OMEGA];

            // remember that this loop happens on many thread concurrently, hence the shared offset counter
            while ((offset = globalSharedBatchOffset.getAndAdd(MSBFSConstants.OMEGA)) < nodeCount) {
                // at the end of the array we might not have a whole omega-sized chunk left, so we resize
                if (offset + MSBFSConstants.OMEGA >= nodeCount) {
                    var numberOfNodesRemaining = (int) (nodeCount - offset);
                    startNodes = new long[numberOfNodesRemaining];
                }

                for (int i = 0; i < startNodes.length; i++) {
                    startNodes[i] = offset + i;
                }

                var msbfsTask = createMSBFSTask(localRelationshipIterators, startNodes);

                msbfsTask.run();
            }
        };
    }

    /**
     * Make concurrent copies of the stack of graphs/ layers so that we may run stuff concurrently
     */
    @NotNull
    private RelationshipIterator[] getRelationshipIterators() {
        var localRelationshipIterators = new RelationshipIterator[relationshipIterators.length];

        for (int i = 0; i < relationshipIterators.length; i++) {
            localRelationshipIterators[i] = relationshipIterators[i].concurrentCopy();
        }

        return localRelationshipIterators;
    }

    private MultiSourceBFSRunnable createMSBFSTask(RelationshipIterator[] localRelationshipIterators, long[] startNodes) {
        var executionStrategy = new TraversalToEdgeMSBFSStrategy(localRelationshipIterators, bfsConsumer);

        return MultiSourceBFSRunnable.createWithoutSeensNext(
            nodeCount,
            null,
            executionStrategy,
            allowSelfLoops,
            startNodes
        );
    }
}
