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
package org.neo4j.gds.labelpropagation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class NonStabilizingLabelPropagationTest {

    @GdlGraph
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a)" +
            ", (b)" +
            ", (c)" +
            ", (d)" +
            ", (e)" +
            ", (f)" +
            ", (g)" +
            ", (h)" +
            ", (g)-[:R]->(a)" +
            ", (a)-[:R]->(d)" +
            ", (d)-[:R]->(b)" +
            ", (b)-[:R]->(e)" +
            ", (e)-[:R]->(c)" +
            ", (c)-[:R]->(f)" +
            ", (f)-[:R]->(h)";

    @Inject
    private Graph graph;

    // According to "Near linear time algorithm to detect community structures in large-scale networks"[1], for a graph of this shape
    // LabelPropagation will not converge unless the iteration is random. However, we don't seem to be affected by this.
    // [1]: https://arxiv.org/pdf/0709.2938.pdf, page 5
    @Test
    void testLabelPropagationDoesStabilize() {
        LabelPropagation labelPropagation = new LabelPropagation(
            graph,
            ImmutableLabelPropagationStreamConfig.builder().build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        LabelPropagationResult compute = labelPropagation.compute();
        compute.labels();
        assertTrue(compute.didConverge(), "Should converge");
    }

}
