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
package org.neo4j.gds.pagerank;

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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.isA;

class PageRankMutateProcTest extends BaseProcTest
    implements MutateNodePropertyTest<PageRankAlgorithm, PageRankMutateConfig, PageRankResult> {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
           "  (a:Label1 {name: 'a'})" +
           ", (b:Label1 {name: 'b'})" +
           ", (c:Label1 {name: 'c'})" +
           ", (d:Label1 {name: 'd'})" +
           ", (e:Label1 {name: 'e'})" +
           ", (f:Label1 {name: 'f'})" +
           ", (g:Label1 {name: 'g'})" +
           ", (h:Label1 {name: 'h'})" +
           ", (i:Label1 {name: 'i'})" +
           ", (j:Label1 {name: 'j'})" +
           ", (b)-[:TYPE1]->(c)" +
           ", (c)-[:TYPE1]->(b)" +
           ", (d)-[:TYPE1]->(a)" +
           ", (d)-[:TYPE1]->(b)" +
           ", (e)-[:TYPE1]->(b)" +
           ", (e)-[:TYPE1]->(d)" +
           ", (e)-[:TYPE1]->(f)" +
           ", (f)-[:TYPE1]->(b)" +
           ", (f)-[:TYPE1]->(e)";

    @Override
    public String mutateProperty() {
        return "score";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE;
    }

    @Override
    public Optional<String> mutateGraphName() {
        return Optional.of("completeGraph");
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            PageRankMutateProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );

        runQuery(
            GdsCypher.call("completeGraph")
                .graphProject()
                .withAnyLabel()
                .withAnyRelationshipType()
                .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a {score: 0.243013d})" +
            ", (b {score: 1.838660d})" +
            ", (c {score: 1.697745d})" +
            ", (d {score: 0.218854d})" +
            ", (e {score: 0.243013d})" +
            ", (f {score: 0.218854d})" +
            ", (g {score: 0.150000d})" +
            ", (h {score: 0.150000d})" +
            ", (i {score: 0.150000d})" +
            ", (j {score: 0.150000d})" +
            ", (b)-[{w: 1.0d}]->(c)" +
            ", (c)-[{w: 1.0d}]->(b)" +
            ", (d)-[{w: 1.0d}]->(a)" +
            ", (d)-[{w: 1.0d}]->(b)" +
            ", (e)-[{w: 1.0d}]->(b)" +
            ", (e)-[{w: 1.0d}]->(d)" +
            ", (e)-[{w: 1.0d}]->(f)" +
            ", (f)-[{w: 1.0d}]->(b)" +
            ", (f)-[{w: 1.0d}]->(e)";
    }

    @Override
    public Class<PageRankMutateProc> getProcedureClazz() {
        return PageRankMutateProc.class;
    }

    @Override
    public PageRankMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return PageRankMutateConfig.of(mapWrapper);
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call("completeGraph")
            .algo("pageRank")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();


        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 10L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "didConverge", false,
            "ranIterations", 20L,
            "centralityDistribution", isA(Map.class),
            "configuration", allOf(isA(Map.class), hasEntry("mutateProperty", mutateProperty()))
        )));
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(PageRankResult result1, PageRankResult result2) {
        assertThat(result1)
            .usingRecursiveComparison()
            .isEqualTo(result2);
    }

}
