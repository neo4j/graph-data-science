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

package org.neo4j.graphalgo.triangle;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.impl.triangle.IntersectingTriangleCount;
import org.neo4j.graphalgo.impl.triangle.TriangleConfig;
import org.neo4j.logging.Log;

public abstract class TriangleBaseProc<CONFIG extends TriangleConfig>
    extends AlgoBaseProc<IntersectingTriangleCount, PagedAtomicIntegerArray, CONFIG> {

    static final String DESCRIPTION =
        "Triangle counting is a community detection graph algorithm that is used to " +
        "determine the number of triangles passing through each node in the graph.";

    @Override
    protected void validateConfigs(GraphCreateConfig graphCreateConfig, CONFIG config) {
        graphCreateConfig.relationshipProjections().projections().entrySet().stream()
            .filter(entry -> entry.getValue().orientation() != Orientation.UNDIRECTED)
            .forEach(entry -> {
                throw new IllegalArgumentException(String.format(
                    "Procedure requires relationship projections to be UNDIRECTED. Projection for `%s` uses orientation `%s`",
                    entry.getKey().name,
                    entry.getValue().orientation()
                ));
            });
    }

    @Override
    protected AlgorithmFactory<IntersectingTriangleCount, CONFIG> algorithmFactory(
        CONFIG config) {
        return new AlphaAlgorithmFactory<IntersectingTriangleCount, CONFIG>() {
            @Override
            public IntersectingTriangleCount buildAlphaAlgo(
                Graph graph,
                CONFIG configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new IntersectingTriangleCount(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency(),
                    AllocationTracker.create()
                )
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }
}
