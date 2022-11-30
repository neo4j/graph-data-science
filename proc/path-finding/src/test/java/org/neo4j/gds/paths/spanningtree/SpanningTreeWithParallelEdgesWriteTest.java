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
package org.neo4j.gds.paths.spanningtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;

public class SpanningTreeWithParallelEdgesWriteTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE (a:Place {name: 'A'})" +
        "CREATE (b:Place {name: 'B'})" +
        "CREATE (a)-[:LINK {cost:800}]->(b)" +
        "CREATE (a)-[:LINK1 {cost:1}]->(b)";


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(SpanningTreeWriteProc.class, GraphProjectProc.class);
    }

    @Test
    void shouldKeepMinimumEdges() {
        var project = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Place")
            .withRelationshipType(
                "LINK",
                Orientation.UNDIRECTED
            )
            .withRelationshipType("LINK1", Orientation.UNDIRECTED)
            .withRelationshipProperty("cost")
            .yields();
        runQuery(project);

        var mstQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.spanningTree")
            .writeMode()
            .addParameter("writeProperty", "writeCost")
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("writeRelationshipType", "MINST")
            .addParameter("sourceNode", 0)
            .yields();
        runQuery(mstQuery);
        
        var validationQuery = "MATCH ()-[r:MINST]->() RETURN r.writeCost AS cost";
        runQueryWithRowConsumer(validationQuery, row -> {
            var cost = (double) row.get("cost");
            assertThat(cost).isEqualTo(1);
        });
    }

}
