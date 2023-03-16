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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

public class LouvainMutateProcTest extends BaseProcTest implements
    MutateNodePropertyTest<Louvain, LouvainMutateConfig, LouvainResult> {

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
            LouvainMutateProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
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

    @Override
    public String mutateProperty() {
        return "communityId";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @Override
    public Optional<String> mutateGraphName() {
        return Optional.of("myGraph");
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a:Node { communityId: 14, seed: 1 })" +
            ", (b:Node { communityId: 14, seed: 1 })" +
            ", (c:Node { communityId: 14, seed: 1 })" +
            ", (d:Node { communityId: 14, seed: 1 })" +
            ", (e:Node { communityId: 14, seed: 1 })" +
            ", (f:Node { communityId: 14, seed: 1 })" +
            ", (g:Node { communityId: 7, seed: 2 })" +
            ", (h:Node { communityId: 7, seed: 2 })" +
            ", (i:Node { communityId: 7, seed: 2 })" +
            ", (j:Node { communityId: 12, seed: 42 })" +
            ", (k:Node { communityId: 12, seed: 42 })" +
            ", (l:Node { communityId: 12, seed: 42 })" +
            ", (m:Node { communityId: 12, seed: 42 })" +
            ", (n:Node { communityId: 12, seed: 42 })" +
            ", (x:Node { communityId: 14, seed: 1 })" +
            // 'LOUVAIN_GRAPH' is UNDIRECTED, e.g. each rel twice
            ", (a)-[:TYPE]->(b)-[:TYPE]->(a)" +
            ", (a)-[:TYPE]->(d)-[:TYPE]->(a)" +
            ", (a)-[:TYPE]->(f)-[:TYPE]->(a)" +
            ", (b)-[:TYPE]->(d)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(x)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(g)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(e)-[:TYPE]->(b)" +
            ", (c)-[:TYPE]->(x)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(f)-[:TYPE]->(c)" +
            ", (d)-[:TYPE]->(k)-[:TYPE]->(d)" +
            ", (e)-[:TYPE]->(x)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(f)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(h)-[:TYPE]->(e)" +
            ", (f)-[:TYPE]->(g)-[:TYPE]->(f)" +
            ", (g)-[:TYPE]->(h)-[:TYPE]->(g)" +
            ", (h)-[:TYPE]->(i)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(j)-[:TYPE]->(h)" +
            ", (i)-[:TYPE]->(k)-[:TYPE]->(i)" +
            ", (j)-[:TYPE]->(k)-[:TYPE]->(j)" +
            ", (j)-[:TYPE]->(m)-[:TYPE]->(j)" +
            ", (j)-[:TYPE]->(n)-[:TYPE]->(j)" +
            ", (k)-[:TYPE]->(m)-[:TYPE]->(k)" +
            ", (k)-[:TYPE]->(l)-[:TYPE]->(k)" +
            ", (l)-[:TYPE]->(n)-[:TYPE]->(l)" +
            ", (m)-[:TYPE]->(n)-[:TYPE]->(m)";
    }

    @Override
    public Class<LouvainMutateProc> getProcedureClazz() {
        return LouvainMutateProc.class;
    }

    @Override
    public LouvainMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainMutateConfig.of(mapWrapper);
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call(mutateGraphName().orElseThrow())
            .algo("louvain")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields(
                "nodePropertiesWritten",
                "preProcessingMillis",
                "computeMillis",
                "mutateMillis",
                "postProcessingMillis",
                "ranLevels",
                "communityCount",
                "modularities",
                "communityDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(15);

                assertThat(row.getNumber("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("computeMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("mutateMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("ranLevels"))
                    .asInstanceOf(LONG)
                    .isEqualTo(2L);

                assertThat(row.getNumber("communityCount"))
                    .asInstanceOf(LONG)
                    .isEqualTo(3L);

                assertThat(row.get("modularities"))
                    .asList()
                    .first(DOUBLE)
                    .isEqualTo(0.376, Offset.offset(1e-3));


                assertThat(row.get("communityDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p99", 7L,
                            "min", 3L,
                            "max", 7L,
                            "mean", 5.0D,
                            "p50", 5L,
                            "p75", 7L,
                            "p90", 7L,
                            "p95", 7L,
                            "p999", 7L
                        )
                    );
            }
        );
    }

    // FIXME: This doesn't belong here.
    @Test
    void zeroCommunitiesInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");

        String graphName = "emptyGraph";

        var loadQuery = GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .yields();

        runQuery(loadQuery);

        String query = GdsCypher
            .call(graphName)
            .algo("louvain")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .yields("communityCount");

        runQueryWithRowConsumer(query, row -> {
           assertThat(row.getNumber("communityCount"))
               .asInstanceOf(LONG)
               .isEqualTo(0L);
        });
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(LouvainResult result1, LouvainResult result2) {
        assertThat(result1)
            .usingRecursiveComparison()
            .isEqualTo(result2);
    }
}
