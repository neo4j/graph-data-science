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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.gds.assertj.ConditionFactory.containsAllEntriesOf;
import static org.neo4j.gds.assertj.ConditionFactory.containsExactlyInAnyOrderEntriesOf;

class LabelPropagationStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {id: 0, seed: 42}) " +
        ", (b:B {id: 1, seed: 42}) " +

        ", (a)-[:X]->(:A {id: 2,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 3,  weight: 2.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 4,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 5,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 6,  weight: 8.0, seed: 2}) " +

        ", (b)-[:X]->(:B {id: 7,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 8,  weight: 2.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 9,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 10, weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 11, weight: 8.0, seed: 2})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            LabelPropagationStatsProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
        // Create explicit graphs with both projection variants
        runQuery(
            "CALL gds.graph.project(" +
            "   'myGraph', " +
            "   {" +
            "       A: {label: 'A', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}, " +
            "       B: {label: 'B', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}" +
            "   }, " +
            "   '*'" +
            ")"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void yields() {
        String query = GdsCypher
            .call("myGraph")
            .algo("labelPropagation")
            .statsMode()
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "didConverge", true,
            "ranIterations", 2L,
            "communityCount", 10L,
            "communityDistribution", containsExactlyInAnyOrderEntriesOf(Map.of(
                "min", 1L,
                "max", 2L,
                "mean", 1.2,
                "p50", 1L,
                "p75", 1L,
                "p90", 2L,
                "p95", 2L,
                "p99", 2L,
                "p999", 2L
            )),
            "preProcessingMillis", greaterThanOrEqualTo(0L),
            "computeMillis", greaterThanOrEqualTo(0L),
            "postProcessingMillis", greaterThanOrEqualTo(0L),
            "configuration", containsAllEntriesOf(MapUtil.map(
                "consecutiveIds", false,
                "maxIterations", 10,
                "seedProperty", null,
                "nodeWeightProperty", null
            ))
        )));
    }

    // FIXME: This doesn't belong here.
    @Test
    void zeroCommunitiesInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .yields();
        runQuery(createQuery);
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("labelPropagation")
            .statsMode()
            .yields("communityCount");

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }
}
