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
package org.neo4j.gds.louvain;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

final class LouvainAlmostEmptyGraphTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(LouvainStreamProc.class, GraphProjectProc.class);
        runQuery("CREATE (:Node)");
        runQuery(GdsCypher.call("myGraph")
            .graphProject()
            .withNodeLabel("Node")
            .withAnyRelationshipType()
            .yields());
    }

    @AfterEach
    void clearCommunities() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStream() {
        runQueryWithRowConsumer(
            GdsCypher.call("myGraph")
                .algo("louvain")
                .streamMode()
                .addParameter("concurrency", 1)
                .addParameter("maxLevels", 5)
                .addParameter("maxIterations", 10)
                .addParameter("tolerance", 0.00001D)
                .addParameter("includeIntermediateCommunities", false)
                .yields(),
            row -> {
                assertThat(row.getNumber("nodeId"))
                    .asInstanceOf(LONG)
                    .isEqualTo(0);
                assertThat(row.getNumber("communityId"))
                    .asInstanceOf(LONG)
                    .isEqualTo(0);
            }
        );
    }
}
