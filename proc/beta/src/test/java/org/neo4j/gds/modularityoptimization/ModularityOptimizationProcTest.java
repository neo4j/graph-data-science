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
package org.neo4j.gds.modularityoptimization;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
abstract class ModularityOptimizationProcTest extends BaseProcTest {

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a', seed1: 0, seed2: 1})" +
        ", (b:Node {name:'b', seed1: 0, seed2: 1})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 1})" +
        ", (d:Node {name:'d', seed1: 2, seed2: 42})" +
        ", (e:Node {name:'e', seed1: 2, seed2: 42})" +
        ", (f:Node {name:'f', seed1: 2, seed2: 42})" +
        ", (a)-[:TYPE {weight: 0.01}]->(b)" +
        ", (a)-[:TYPE {weight: 5.0}]->(e)" +
        ", (a)-[:TYPE {weight: 5.0}]->(f)" +
        ", (b)-[:TYPE {weight: 5.0}]->(c)" +
        ", (b)-[:TYPE {weight: 5.0}]->(d)" +
        ", (c)-[:TYPE {weight: 0.01}]->(e)" +
        ", (f)-[:TYPE {weight: 0.01}]->(d)";

    static final long[][] UNWEIGHTED_COMMUNITIES = {new long[]{0, 1, 2, 4}, new long[]{3, 5}};
    static final long[][] WEIGHTED_COMMUNITIES = {new long[]{0, 4, 5}, new long[]{1, 2, 3}};
    static final long[][] SEEDED_COMMUNITIES = {new long[]{0, 1}, new long[]{2, 3, 4, 5}};

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ModularityOptimizationStreamProc.class,
            ModularityOptimizationWriteProc.class,
            ModularityOptimizationMutateProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    GdsCypher.ModeBuildStage algoBuildStage() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME);
        return GdsCypher.call(DEFAULT_GRAPH_NAME).algo("gds", "beta", "modularityOptimization");
    }
}
