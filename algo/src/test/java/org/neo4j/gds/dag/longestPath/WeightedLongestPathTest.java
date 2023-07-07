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
package org.neo4j.gds.dag.longestPath;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class WeightedLongestPathTest {
    private static LongestPathBaseConfig CONFIG = new LongestPathStreamConfigImpl.Builder()
        .concurrency(4)
        .build();

    @GdlGraph(graphNamePrefix = "basic")
    private static final String basicQuery =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n0)-[:T {prop: 8.0}]->(n1)" +
        ", (n0)-[:T {prop: 5.0}]->(n2)" +
        ", (n2)-[:T {prop: 2.0}]->(n1)" +
        ", (n3)-[:T {prop: 8.0}]->(n0)";

    @Inject
    private TestGraph basicGraph;

    @Test
    void basicWeightedLongestPath() {
        LongestPath ts = new LongestPath(basicGraph, CONFIG, ProgressTracker.NULL_TRACKER);
        TopologicalSortResult result = ts.compute();

        var longestPathsDistances = result.longestPathDistances().get();
        var firstLongestPathDistance = longestPathsDistances.get(0);
        var secondLongestPathDistance = longestPathsDistances.get(1);
        var thirdLongestPathDistance = longestPathsDistances.get(2);
        var fourthLongestPathDistance = longestPathsDistances.get(3);

        assertEquals(8.0, firstLongestPathDistance);
        assertEquals(16.0, secondLongestPathDistance);
        assertEquals(13.0, thirdLongestPathDistance);
        assertEquals(0.0, fourthLongestPathDistance);
    }
}
