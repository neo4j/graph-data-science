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
package org.neo4j.gds.influenceMaximization;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

/**
 *     (c)-----|
 *    /(d)\----|-|
 *   //(e)\\---|-|-|
 *  ///(f)\\\--|-|-|-|
 * ////   \\\\ | | | |
 * (a)     (b) | | | |
 * \\\\   //// | | | |
 *  \\\(g)///--| | | |
 *   \\(h)//-----| | |
 *    \(i)/--------| |
 *     (j)-----------|
 */
class CELFStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
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

        ", (a)-[:RELATIONSHIP]->(c)" +
        ", (a)-[:RELATIONSHIP]->(d)" +
        ", (a)-[:RELATIONSHIP]->(e)" +
        ", (a)-[:RELATIONSHIP]->(f)" +
        ", (a)-[:RELATIONSHIP]->(g)" +
        ", (a)-[:RELATIONSHIP]->(h)" +
        ", (a)-[:RELATIONSHIP]->(i)" +
        ", (a)-[:RELATIONSHIP]->(j)" +

        ", (b)-[:RELATIONSHIP]->(c)" +
        ", (b)-[:RELATIONSHIP]->(d)" +
        ", (b)-[:RELATIONSHIP]->(e)" +
        ", (b)-[:RELATIONSHIP]->(f)" +
        ", (b)-[:RELATIONSHIP]->(g)" +
        ", (b)-[:RELATIONSHIP]->(h)" +
        ", (b)-[:RELATIONSHIP]->(i)" +
        ", (b)-[:RELATIONSHIP]->(j)" +

        ", (c)-[:RELATIONSHIP]->(g)" +
        ", (d)-[:RELATIONSHIP]->(h)" +
        ", (e)-[:RELATIONSHIP]->(i)" +
        ", (f)-[:RELATIONSHIP]->(j)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(CELFStatsProc.class, GraphProjectProc.class);

        String graphCreateQuery = GdsCypher.call("celfGraph")
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType(
                "RELATIONSHIP",
                RelationshipProjection.of(
                    "RELATIONSHIP",
                    Orientation.NATURAL,
                    Aggregation.DEFAULT
                )
            ).yields();

        runQuery(graphCreateQuery);
    }

    @AfterEach
    void shutdownGraph() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void stats() {

        var cypher = GdsCypher.call("celfGraph")
            .algo("gds.beta.influenceMaximization.celf")
            .statsMode()
            .addParameter("seedSetSize", 5)
            .addParameter("propagationProbability", 0.2)
            .addParameter("monteCarloSimulations", 10)
            .yields("computeMillis", "totalSpread");

        runQueryWithRowConsumer(cypher, (tx, row) -> {
            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThanOrEqualTo(0L);

            assertThat(row.getNumber("totalSpread"))
                .asInstanceOf(DOUBLE)
                .isCloseTo(6.4, Offset.offset(1e-7));

        });
    }
}
