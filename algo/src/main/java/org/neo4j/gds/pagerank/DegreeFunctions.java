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
package org.neo4j.gds.pagerank;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongToDoubleFunction;

public final class DegreeFunctions {

    private DegreeFunctions() {}

    public static @NotNull LongToDoubleFunction pageRankDegreeFunction(
        Graph graph,
        boolean hasRelationshipWeightProperty,
        Concurrency concurrency
    ) {
        var degreeCentrality = new DegreeCentrality(
            graph,
            DefaultPool.INSTANCE,
            concurrency,
            Orientation.NATURAL,
            hasRelationshipWeightProperty,
            10_000,
            ProgressTracker.NULL_TRACKER
        );

        var degrees = degreeCentrality.compute().degreeFunction();
        return degrees::get;
    }

    public static @NotNull LongToDoubleFunction eigenvectorDegreeFunction(
        Graph graph,
        boolean hasRelationshipWeightProperty,
        Concurrency concurrency
    ) {
        if (hasRelationshipWeightProperty) {
            var degreeCentrality = new DegreeCentrality(
                graph,
                DefaultPool.INSTANCE,
                concurrency,
                Orientation.NATURAL,
                true,
                10_000,
                ProgressTracker.NULL_TRACKER
            );

            var degrees = degreeCentrality.compute().degreeFunction();
            return degrees::get;
        }

        return (nodeId) -> 1;
    }


    public static double averageDegree(Graph graph, Concurrency concurrency) {
        var degreeSum = new LongAdder();
        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            nodeId -> degreeSum.add(graph.degree(nodeId))
        );
        return (double) degreeSum.sum() / graph.nodeCount();
    }

}
