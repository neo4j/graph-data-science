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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.CommunityHelper.assertCommunities;

class LouvainStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {seed: 1})" +        // 0
        ", (b:Node {seed: 1})" +        // 1
        ", (c:Node {seed: 1})" +        // 2
        ", (d:Node {seed: 1})" +        // 3
        ", (e:Node {seed: 1})" +        // 4
        ", (f:Node {seed: 1})" +        // 5
        ", (g:Node {seed: 2})" +        // 6
        ", (h:Node {seed: 2})" +        // 7
        ", (i:Node {seed: 2})" +        // 8
        ", (j:Node {seed: 42})" +       // 9
        ", (k:Node {seed: 42})" +       // 10
        ", (l:Node {seed: 42})" +       // 11
        ", (m:Node {seed: 42})" +       // 12
        ", (n:Node {seed: 42})" +       // 13
        ", (x:Node {seed: 1})" +        // 14

        ", (a)-[:TYPE {weight: 1.0}]->(b)" +
        ", (a)-[:TYPE {weight: 1.0}]->(d)" +
        ", (a)-[:TYPE {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE {weight: 1.0}]->(x)" +
        ", (b)-[:TYPE {weight: 1.0}]->(g)" +
        ", (b)-[:TYPE {weight: 1.0}]->(e)" +
        ", (c)-[:TYPE {weight: 1.0}]->(x)" +
        ", (c)-[:TYPE {weight: 1.0}]->(f)" +
        ", (d)-[:TYPE {weight: 1.0}]->(k)" +
        ", (e)-[:TYPE {weight: 1.0}]->(x)" +
        ", (e)-[:TYPE {weight: 0.01}]->(f)" +
        ", (e)-[:TYPE {weight: 1.0}]->(h)" +
        ", (f)-[:TYPE {weight: 1.0}]->(g)" +
        ", (g)-[:TYPE {weight: 1.0}]->(h)" +
        ", (h)-[:TYPE {weight: 1.0}]->(i)" +
        ", (h)-[:TYPE {weight: 1.0}]->(j)" +
        ", (i)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(m)" +
        ", (j)-[:TYPE {weight: 1.0}]->(n)" +
        ", (k)-[:TYPE {weight: 1.0}]->(m)" +
        ", (k)-[:TYPE {weight: 1.0}]->(l)" +
        ", (l)-[:TYPE {weight: 1.0}]->(n)" +
        ", (m)-[:TYPE {weight: 1.0}]->(n)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            LouvainStreamProc.class,
            GraphProjectProc.class
        );

        runQuery(
            "CALL gds.graph.project(" +
            "  'myGraph', " +
            "  {Node: {label: 'Node', properties: 'seed'}}, " +
            "  {TYPE: {type: 'TYPE', orientation: 'UNDIRECTED'}}" +
            ")"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStream() {
        String query = "CALL gds.louvain.stream('myGraph', {}) " +
                       " YIELD nodeId, communityId, intermediateCommunityIds";

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            assertThat(row.get("intermediateCommunityIds")).isNull();
            actualCommunities.add(id, community);
        });
        assertCommunities(actualCommunities, List.of(
            List.of(0L, 1L, 2L, 3L, 4L, 5L, 14L),
            List.of(6L, 7L, 8L),
            List.of(9L, 10L, 11L, 12L, 13L)
        ));
    }

    @Test
    void testStreamConsecutiveIds() {
        String query = "CALL gds.louvain.stream('myGraph', {consecutiveIds: true}) " +
                       " YIELD nodeId, communityId, intermediateCommunityIds";

        var communitySet = new HashSet<Long>();

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            communitySet.add(community);
            assertThat(row.get("intermediateCommunityIds")).isNull();
            actualCommunities.add(id, community);
        });
        assertCommunities(actualCommunities, List.of(
            List.of(0L, 1L, 2L, 3L, 4L, 5L, 14L),
            List.of(6L, 7L, 8L),
            List.of(9L, 10L, 11L, 12L, 13L)
        ));
        assertThat(communitySet).hasSize(3).containsExactlyInAnyOrder(0L, 1L, 2L);
    }

    @Test
    void testStreamCommunities() {
        String query = "CALL gds.louvain.stream('myGraph', {includeIntermediateCommunities: true}) " +
                       " YIELD nodeId, communityId, intermediateCommunityIds";

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("intermediateCommunityIds"))
                .asList()
                .hasSize(2)
                .last(LONG)
                .isEqualTo(row.getNumber("communityId").longValue());
        });
    }

    @Test
    void testStreamNodeFiltered() {
        var graphName = "multi_label";

        runQuery("CREATE (:Ignored)");

        long totalNodeCount = runQuery(
            "MATCH (n) RETURN count(n) AS count",
            result -> (Long) result.next().get("count")
        );
        long filteredNodeCount = runQuery(
            "MATCH (n:Node) RETURN count(n) AS count",
            result -> (Long) result.next().get("count")
        );

        assertThat(totalNodeCount).isGreaterThan(filteredNodeCount);

        // project graph including the to-be-ignored label
        runQuery(GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel("Node")
            .withNodeLabel("Ignored")
            .withAnyRelationshipType()
            .yields());

        @Language("Cypher") String query = GdsCypher.call(graphName)
            .algo("louvain")
            .streamMode()
            // execute Louvain on node subset
            .addParameter("nodeLabels", List.of("Node"))
            .yields("nodeId");

        var actualFilteredCount = runQuery(
            query + " RETURN count(nodeId) AS count",
            result -> (Long) result.next().get("count")
        );

        assertThat(actualFilteredCount).isEqualTo(filteredNodeCount);
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
            Arguments.of(Map.of("minCommunitySize", 1), List.of(0, 1, 2), List.of(0, 1, 2)),
            Arguments.of(Map.of("minCommunitySize", 5), List.of(0, 1), List.of(0, 2))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamMinCommunitySize(
        Map<String, Long> parameters,
        List<Integer> expectedCommunityIds,
        List<Integer> mappedCommunityIds
    ) {
        @Language("Cypher") String query = GdsCypher.call("myGraph")
            .algo("louvain")
            .streamMode()
            .addParameter("consecutiveIds", true)
            .addAllParameters(parameters)
            .yields("nodeId", "communityId");

        var communitySet = new HashSet<Integer>();

        runQueryWithRowConsumer(query, row -> {
            long id = row.getNumber("nodeId").longValue();
            int community = row.getNumber("communityId").intValue();
            int mappedCommunityId = mappedCommunityIds.get(community);
            communitySet.add(community);
            assertThat(List.of(
                List.of(0L, 1L, 2L, 3L, 4L, 5L, 14L),
                List.of(6L, 7L, 8L),
                List.of(9L, 10L, 11L, 12L, 13L)
            ).get(mappedCommunityId)).contains(id);
        });
        assertThat(communitySet).containsExactlyElementsOf(expectedCommunityIds);
    }
}
