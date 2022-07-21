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
package org.neo4j.gds.paths.sourcetarget;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateRelationshipWithPropertyTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.config.MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;

class ShortestPathYensMutateProcTest extends ShortestPathYensProcTest<ShortestPathYensMutateConfig>
    implements MutateRelationshipWithPropertyTest<Yens, ShortestPathYensMutateConfig, DijkstraResult> {

    private static final String EXISTING_GRAPH =
        "CREATE" +
        ", (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        ", (g:Label)" +
        ", (h:Label)" +
        ", (c)-[{w: 3.0}]->(d)" +
        ", (c)-[{w: 2.0}]->(e)" +
        ", (d)-[{w: 4.0}]->(f)" +
        ", (e)-[{w: 1.0}]->(d)" +
        ", (e)-[{w: 2.0}]->(f)" +
        ", (e)-[{w: 3.0}]->(g)" +
        ", (f)-[{w: 2.0}]->(g)" +
        ", (f)-[{w: 1.0}]->(h)" +
        ", (g)-[{w: 2.0}]->(h)";

    @Override
    public String expectedMutatedGraph() {
        return EXISTING_GRAPH +
               // new relationship as a result from mutate
               ", (c)-[:PATH {w: 3.0}]->(h)" +
               ", (c)-[:PATH {w: 3.0}]->(h)" +
               ", (c)-[:PATH {w: 3.0}]->(h)";
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
    public Class<? extends AlgoBaseProc<Yens, DijkstraResult, ShortestPathYensMutateConfig, ?>> getProcedureClazz() {
        return ShortestPathYensMutateProc.class;
    }

    @Override
    public ShortestPathYensMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathYensMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey(MUTATE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(MUTATE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
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

        var query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.shortestPath.yens")
            .mutateMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 3L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog.get(getUsername(), databaseId(), "graph").graphStore().getUnion();
        var expected = TestSupport.fromGdl(
            EXISTING_GRAPH +
            // new relationship as a result from mutate
            ", (c)-[:PATH {w: 5.0}]->(h)" +
            ", (c)-[:PATH {w: 7.0}]->(h)" +
            ", (c)-[:PATH {w: 8.0}]->(h)"
        );

        assertGraphEquals(expected, actual);
    }
}
