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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.PRUNED;
import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.ROOT_NODE;

abstract class ReroutingAlgorithm {

    protected final Graph graph;
    protected final long sourceId;
    protected final int concurrency;
    protected final ProgressTracker progressTracker;

    ReroutingAlgorithm(
        Graph graph,
        long sourceId,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.progressTracker = progressTracker;
        this.concurrency = concurrency;
        this.sourceId = sourceId;
    }

    LinkCutTree createLinkCutTree(HugeLongArray parent) {
        var tree = new LinkCutTree(graph.nodeCount());
        for (long nodeId = 0; nodeId < graph.nodeCount(); ++nodeId) {
            var parentId = parent.get(nodeId);
            if (parentId != PRUNED && parentId != ROOT_NODE) {
                tree.link(parentId, nodeId);
            }
        }
        return tree;
    }

    abstract void reroute(
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
    );

    boolean checkIfRerouteIsValid(
        LinkCutTree tree,
        long source,
        long target,
        long parentTarget
    ) {
        //we want to check if the edge source->target causes a loop
        //i.e.,  target is a predecessor of source
        //just checking if source is connected to target is not valid (this is a spanning tree all are connected)
        //we use the LinkCutTree and cut the  target's parent. If the two are connected still, we'll have a loop
        //Example:
        //    pp->target-> a->b->->source
        // after cutting pp->target
        //  pp  target-> a->b->source
        // We have loop ==> so source->target is not a viable replacement

        //Otherwise, assuming no loop,  there is a root-source path not involving target. So by  setting source->target
        //all terminals with  target as predecessor can still reach be reached by source.
        tree.delete(parentTarget, target);
        return !tree.connected(source, target);
    }

    void reconnect(
        LinkCutTree tree,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        long source,
        long target,
        double weight
    ) {
        double edgeCostOft = parentCost.get(target);
        parent.set(target, source);
        parentCost.set(target, weight);
        totalCost.add(-edgeCostOft + weight); //remove old cost, add new cost due to relinking
        tree.link(source, target); // update tree
    }

    static ReroutingAlgorithm createRerouter(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        BitSet isTerminal,
        HugeLongArray examinationQueue,
        LongAdder indexQueue,
        int concurrency,
        ProgressTracker progressTracker
    ) {

        if (graph.characteristics().isInverseIndexed() && !graph.characteristics().isUndirected()) {
            return new InverseRerouter(
                graph,
                sourceId,
                isTerminal,
                examinationQueue,
                indexQueue,
                concurrency,
                progressTracker
            );
        } else {
            return new SimpleRerouter(graph, sourceId, terminals, concurrency, progressTracker);
        }
    }
}
