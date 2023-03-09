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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.transaction.TransactionContext;

import static org.assertj.core.api.Assertions.assertThat;

class CypherQueryEstimatorTest extends BaseTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a1:A {property: 33, a: 33})" +
        ", (a2:A {property: 33, a: 33})" +
        ", (b:B {property: 42, b: 42})" +
        ", (a1)-[:T1 {property1: 42, property2: 1337}]->(b)" +
        ", (a2)-[:T2 {property1: 43}]->(b)" +
        ", (a2)-[:T2 {property1: 43}]->(a1)";

    @Test
    void estimateNodes() {
        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            CypherQueryEstimator estimator = new CypherQueryEstimator(TransactionContext.of(db, tx));

            var estimation = estimator.getNodeEstimation(
                "MATCH (n) RETURN id(n) AS id, labels(n) AS labels, n.property AS score");

            // EXPLAIN seems to overestimate the nodeCount here
            assertThat(estimation).isEqualTo(ImmutableEstimationResult.of(10, 1));
        });
    }

    @Test
    void estimateRelationships() {
        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            CypherQueryEstimator estimator = new CypherQueryEstimator(TransactionContext.of(db, tx));

            var estimation = estimator.getRelationshipEstimation(
                "MATCH (n)-[r]-(m) RETURN id(n) AS source, m AS target, r.property1 AS score, type(r) AS type");

            assertThat(estimation).isEqualTo(ImmutableEstimationResult.of(6, 1));
        });
    }


}
