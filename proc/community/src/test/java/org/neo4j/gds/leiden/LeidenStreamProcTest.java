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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class LeidenStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +
        "  (a5:Node)," +
        "  (a6:Node)," +
        "  (a7:Node)," +
        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: 1.0}]->(a2)," +
        "  (a0)-[:R {weight: 1.0}]->(a3)," +
        "  (a0)-[:R {weight: 1.0}]->(a4)," +
        "  (a2)-[:R {weight: 1.0}]->(a3)," +
        "  (a2)-[:R {weight: 1.0}]->(a4)," +
        "  (a3)-[:R {weight: 1.0}]->(a4)," +
        "  (a1)-[:R {weight: 1.0}]->(a5)," +
        "  (a1)-[:R {weight: 1.0}]->(a6)," +
        "  (a1)-[:R {weight: 1.0}]->(a7)," +
        "  (a5)-[:R {weight: 1.0}]->(a6)," +
        "  (a5)-[:R {weight: 1.0}]->(a7)," +
        "  (a6)-[:R {weight: 1.0}]->(a7)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            LeidenStreamProc.class
        );

        var projectQuery = GdsCypher.call("leiden").graphProject().loadEverything(Orientation.UNDIRECTED).yields();
        runQuery(projectQuery);
    }

    @Test
    void stream() {
        runQuery("CALL gds.alpha.leiden.stream('leiden')", result -> {
            assertThat(result.columns()).containsExactlyInAnyOrder("nodeId", "communityId", "intermediateCommunityIds");
            long resultRowCount = 0;
            var communities = new HashSet<Long>();
            while(result.hasNext()) {
                var next = result.next();
                assertThat(next.get("nodeId")).isInstanceOf(Long.class);
                assertThat(next.get("communityId")).isInstanceOf(Long.class);
                assertThat(next.get("intermediateCommunityIds")).isNull();
                communities.add((Long) next.get("communityId"));
                resultRowCount++;
            }
            assertThat(resultRowCount).isEqualTo(8L);
            assertThat(communities).hasSize(2);
            return true;
        });
    }

    @Test
    void streamWithIntermediateCommunityIds() {
        runQuery("CALL gds.alpha.leiden.stream('leiden', {includeIntermediateCommunities: true}) ", result -> {
            assertThat(result.columns()).containsExactlyInAnyOrder("nodeId", "communityId", "intermediateCommunityIds");
            long resultRowCount = 0;
            var communities = new HashSet<Long>();
            while (result.hasNext()) {
                var next = result.next();
                assertThat(next.get("nodeId")).isInstanceOf(Long.class);
                assertThat(next.get("communityId")).isInstanceOf(Long.class);
                assertThat(next.get("intermediateCommunityIds")).isInstanceOf(List.class);
                List<Long> intermediateCommunities = (List<Long>) next.get("intermediateCommunityIds");
                assertThat(intermediateCommunities).hasSize(1);
                assertThat(intermediateCommunities.get(intermediateCommunities.size() - 1)).isEqualTo((long) next.get(
                    "communityId"));
                communities.add((Long) next.get("communityId"));
                resultRowCount++;
            }
            assertThat(resultRowCount).isEqualTo(8L);
            assertThat(communities).hasSize(2);
            return true;
        });
    }

    @Test
    void shouldStreamWithConsecutiveIds() {
        runQuery("CALL gds.alpha.leiden.stream('leiden', {consecutiveIds: true})", result -> {
            assertThat(result.columns()).containsExactlyInAnyOrder("nodeId", "communityId", "intermediateCommunityIds");
            long resultRowCount = 0;
            var communities = new HashSet<Long>();
            while (result.hasNext()) {
                var next = result.next();
                assertThat(next.get("nodeId")).isInstanceOf(Long.class);
                assertThat(next.get("communityId")).isInstanceOf(Long.class);
                assertThat(next.get("intermediateCommunityIds")).isNull();
                communities.add((Long) next.get("communityId"));
                resultRowCount++;
            }
            assertThat(resultRowCount).isEqualTo(8L);
            assertThat(communities).hasSize(2);
            assertThat(communities).containsExactly(0L, 1L);
            return true;
        });
    }

    @Test
    void shouldEstimateMemory() {
        var query = "CALL gds.alpha.leiden.stream.estimate('leiden', {" +
                    "   includeIntermediateCommunities: true" +
                    "})";
        assertThatNoException().isThrownBy(() -> runQuery(query));
    }
}
