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
package org.neo4j.gds.leiden;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.ProcedureConstants.TOLERANCE_DEFAULT;

@GdlExtension
class LeidenWeightedCliqueTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE" +
        "(a : Node), "+
        "(b : Node), "+
        "(c : Node), "+
        "(d : Node), "+
        "(e : Node), "+

        "(a)-[:R {w: 0.1}]->(b),"+
        "(a)-[:R {w: 0.1}]->(c),"+
        "(a)-[:R {w: 0.1}]->(d),"+
        "(a)-[:R {w: 0.1}]->(e),"+

        "(b)-[:R {w: 0.1}]->(c),"+
        "(b)-[:R {w: 0.1}]->(d),"+
        "(b)-[:R {w: 0.1}]->(e),"+

        "(c)-[:R {w: 0.1}]->(d),"+
        "(c)-[:R {w: 0.1}]->(e),"+

        "(d)-[:R {w: 0.1}]->(e)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void weightedLeiden() {
        var maxLevels = 10;

        Leiden leiden = new Leiden(
            graph,
            maxLevels,
            1.0,
            0.01,
            true,
            19L,
            null,
            TOLERANCE_DEFAULT,
            4,
            ProgressTracker.NULL_TRACKER

        );
        var leidenResult = leiden.compute();
        assertThat(Arrays.stream(leidenResult.communities().toArray()).distinct().count()).isEqualTo(1);

    }

}
