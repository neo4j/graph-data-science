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
package org.neo4j.gds.paths.singlesource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.MutateRelationshipWithPropertyTest;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.config.MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY;

class AllShortestPathsDijkstraMutateProcTest extends AllShortestPathsDijkstraProcTest<AllShortestPathsDijkstraMutateConfig>
    implements MutateRelationshipWithPropertyTest<Dijkstra, AllShortestPathsDijkstraMutateConfig, DijkstraResult> {

    private static final String EXISTING_GRAPH =
        "CREATE" +
        "  (a:Label)" +
        ", (b:Label)" +
        ", (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        "  (a)-[{w: 4.0D}]->(b)" +
        ", (a)-[{w: 2.0D}]->(c)" +
        ", (b)-[{w: 5.0D}]->(c)" +
        ", (b)-[{w: 10.0D}]->(d)" +
        ", (c)-[{w: 3.0D}]->(e)" +
        ", (d)-[{w: 11.0D}]->(f)" +
        ", (e)-[{w: 4.0D}]->(d)";

    @Override
    public String expectedMutatedGraph() {
        return EXISTING_GRAPH +
               // new relationship as a result from mutate
               ", (a)-[{w: 0.0D}]->(a)" +
               ", (a)-[{w: 1.0D}]->(c)" +
               ", (a)-[{w: 1.0D}]->(b)" +
               ", (a)-[{w: 2.0D}]->(e)" +
               ", (a)-[{w: 2.0D}]->(d)" +
               ", (a)-[{w: 3.0D}]->(f)";
    }

    @Override
    public String mutateRelationshipType() {
        return WRITE_RELATIONSHIP_TYPE;
    }

    @Override
    public String mutateProperty() {
        return null;
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE;
    }

    public Optional<String> mutateGraphName() {
        return Optional.of(GRAPH_NAME);
    }

    @Override
    public Class<? extends AlgoBaseProc<Dijkstra, DijkstraResult, AllShortestPathsDijkstraMutateConfig>> getProcedureClazz() {
        return AllShortestPathsDijkstraMutateProc.class;
    }

    @Override
    public AllShortestPathsDijkstraMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return AllShortestPathsDijkstraMutateConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey(MUTATE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(MUTATE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
    }

    @BeforeEach
    void setup() throws Exception {
        super.setup();
        registerProcedures(
            GraphWriteNodePropertiesProc.class
        );
    }

    @Override
    @Test
    @Disabled("This test does not work for Dijkstra as no property is written")
    public void testMutateFailsOnExistingToken() {}


    @Override
    @Test
    @Disabled("This test does not work for Dijkstra as the source node is filtered")
    public void testGraphMutationOnFilteredGraph() {}

    @Override
    @Test
    @Disabled("This test does not work for Dijkstra as the source node is filtered")
    public void testWriteBackGraphMutationOnFilteredGraph() {}

    @Test
    void testWeightedMutate() {
        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call().explicitCreation("graph")
            .algo("gds.allShortestPaths.dijkstra")
            .mutateMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("mutateRelationshipType", mutateRelationshipType())
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 6L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog.get(getUsername(), namedDatabaseId(), "graph").graphStore().getUnion();
        var expected = TestSupport.fromGdl(
            EXISTING_GRAPH +
            // new relationship as a result from mutate
            ", (a)-[{w: 0.0D}]->(a)" +
            ", (a)-[{w: 2.0D}]->(c)" +
            ", (a)-[{w: 4.0D}]->(b)" +
            ", (a)-[{w: 5.0D}]->(e)" +
            ", (a)-[{w: 9.0D}]->(d)" +
            ", (a)-[{w: 20.0D}]->(f)"
        );

        assertGraphEquals(expected, actual);
    }
}
