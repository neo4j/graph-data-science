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
package org.neo4j.graphalgo.beta.pregel.pr;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.HashMap;

import static org.neo4j.graphalgo.TestSupport.assertDoubleValues;
import static org.neo4j.graphalgo.beta.pregel.pr.PageRankPregel.PAGE_RANK;

@GdlExtension
class PageRankPregelAlgoTest {

    // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
    @GdlGraph
    private static final String TEST_GRAPH =
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (g:Node)" +
            ", (h:Node)" +
            ", (i:Node)" +
            ", (j:Node)" +
            ", (k:Node)" +
            ", (b)-[:REL]->(c)" +
            ", (c)-[:REL]->(b)" +
            ", (d)-[:REL]->(a)" +
            ", (d)-[:REL]->(b)" +
            ", (e)-[:REL]->(b)" +
            ", (e)-[:REL]->(d)" +
            ", (e)-[:REL]->(f)" +
            ", (f)-[:REL]->(b)" +
            ", (f)-[:REL]->(e)" +
            ", (g)-[:REL]->(b)" +
            ", (g)-[:REL]->(e)" +
            ", (h)-[:REL]->(b)" +
            ", (h)-[:REL]->(e)" +
            ", (i)-[:REL]->(b)" +
            ", (i)-[:REL]->(e)" +
            ", (j)-[:REL]->(e)" +
            ", (k)-[:REL]->(e)";

    @Inject
    private TestGraph graph;

    @Test
    void runPR() {
        int maxIterations = 10;
        float dampingFactor = 0.85f;

        var config = ImmutablePageRankPregelConfig.builder()
            .maxIterations(maxIterations)
            .dampingFactor(dampingFactor)
            .isAsynchronous(false)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new PageRankPregel(),
            Pools.DEFAULT,
            AllocationTracker.empty(),
            ProgressLogger.NULL_LOGGER
        );

        HugeDoubleArray nodeValues = pregelJob.run().nodeValues().doubleProperties(PAGE_RANK);

        var expected = new HashMap<String, Double>();
        expected.put("a", 0.0276D);
        expected.put("b", 0.3483D);
        expected.put("c", 0.2650D);
        expected.put("d", 0.0330D);
        expected.put("e", 0.0682D);
        expected.put("f", 0.0330D);
        expected.put("g", 0.0136D);
        expected.put("h", 0.0136D);
        expected.put("i", 0.0136D);
        expected.put("j", 0.0136D);
        expected.put("k", 0.0136D);

        assertDoubleValues(graph, nodeValues::get, expected, 1E-3);
    }
}
