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
package org.neo4j.gds.approxmaxkcut;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.fromGdl;

class ApproxMaxKCutMutateProcTest extends BaseProcTest implements
    MutateNodePropertyTest<ApproxMaxKCut, ApproxMaxKCutMutateConfig, ApproxMaxKCut.CutResult>,
    MemoryEstimateTest<ApproxMaxKCut, ApproxMaxKCutMutateConfig, ApproxMaxKCut.CutResult> {

    @Neo4jGraph
    @Language("Cypher")
    private static final
    String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +

        ", (a)-[:TYPE1 {weight: 81.0}]->(b)" +
        ", (a)-[:TYPE1 {weight: 7.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(e)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(g)" +
        ", (c)-[:TYPE1 {weight: 45.0}]->(b)" +
        ", (c)-[:TYPE1 {weight: 3.0}]->(e)" +
        ", (d)-[:TYPE1 {weight: 3.0}]->(c)" +
        ", (d)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 3.0}]->(a)" +
        ", (f)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 4.0}]->(c)";

    static final String GRAPH_NAME = "myGraph";

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            ApproxMaxKCutMutateProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .loadEverything()
            .yields();

        runQuery(createQuery);
    }

    @Override
    public String mutateProperty() {
        return "community";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
    }

    @Override
    public Class<ApproxMaxKCutMutateProc> getProcedureClazz() {
        return ApproxMaxKCutMutateProc.class;
    }

    @Override
    public ApproxMaxKCutMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ApproxMaxKCutMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return mapWrapper
            .withEntryIfMissing("k", 2)
            .withEntryIfMissing("iterations", 8)
            .withEntryIfMissing("vnsMaxNeighborhoodOrder", 0)
            .withEntryIfMissing("concurrency", 1)
            .withEntryIfMissing("randomSeed", 1337L)
            .withEntryIfMissing("mutateProperty", this.mutateProperty());
    }

    // We override this in order to be able to specify an algo config yielding a deterministic result.
    @Override
    @Test
    public void testGraphMutation() {
        GraphStore graphStore = runMutation(ensureGraphExists(), createMinimalConfig(CypherMapWrapper.empty()));

        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), graphStore.getUnion());
        GraphSchema schema = graphStore.schema();
        if (mutateProperty() != null) {
            boolean nodesContainMutateProperty = containsMutateProperty(schema.nodeSchema());
            boolean relationshipsContainMutateProperty = containsMutateProperty(schema.relationshipSchema());
            assertTrue(nodesContainMutateProperty || relationshipsContainMutateProperty);
        }
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a { community: 1 })" +
            ", (b { community: 1 })" +
            ", (c { community: 1 })" +
            ", (d { community: 0 })" +
            ", (e { community: 0 })" +
            ", (f { community: 0 })" +
            ", (g { community: 0 })" +

            ", (a)-[{w: 1.0d}]->(b)" +
            ", (a)-[{w: 1.0d}]->(d)" +
            ", (b)-[{w: 1.0d}]->(d)" +
            ", (b)-[{w: 1.0d}]->(e)" +
            ", (b)-[{w: 1.0d}]->(f)" +
            ", (b)-[{w: 1.0d}]->(g)" +
            ", (c)-[{w: 1.0d}]->(b)" +
            ", (c)-[{w: 1.0d}]->(e)" +
            ", (d)-[{w: 1.0d}]->(c)" +
            ", (d)-[{w: 1.0d}]->(b)" +
            ", (e)-[{w: 1.0d}]->(b)" +
            ", (f)-[{w: 1.0d}]->(a)" +
            ", (f)-[{w: 1.0d}]->(b)" +
            ", (g)-[{w: 1.0d}]->(b)" +
            ", (g)-[{w: 1.0d}]->(c)";
    }

    @Override
    public void assertResultEquals(ApproxMaxKCut.CutResult result1, ApproxMaxKCut.CutResult result2) {
        assertThat(result1.cutCost())
            .isEqualTo(result2.cutCost());
    }

    @AfterEach
    void clearStore() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

}
