/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.paths.sourcetarget;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.yens.Yens;
import org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.beta.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;
import static org.neo4j.graphalgo.config.MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY;

class ShortestPathYensMutateProcTest extends ShortestPathYensProcTest<ShortestPathYensMutateConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Yens, DijkstraResult, ShortestPathYensMutateConfig>> getProcedureClazz() {
        return ShortestPathYensMutateProc.class;
    }

    @Override
    public ShortestPathYensMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ShortestPathYensMutateConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey(MUTATE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(MUTATE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
    }

    @Test
    void testMutate() {
        var relationshipWeightProperty = "cost";
        var graphName = "graph";

        var config = createConfig(createMinimalConfig(CypherMapWrapper.empty()));

        var createQuery = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperty(relationshipWeightProperty)
            .graphCreate(graphName)
            .yields();

        runQuery(createQuery);

        var query = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.beta.shortestPath.yens")
            .mutateMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 3L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog.get(getUsername(), namedDatabaseId(), graphName).graphStore().getUnion();
        var expected = TestSupport.fromGdl(
            "CREATE" +
            ", (c)-[{w: 3.0}]->(d)" +
            ", (c)-[{w: 2.0}]->(e)" +
            ", (d)-[{w: 4.0}]->(f)" +
            ", (e)-[{w: 1.0}]->(d)" +
            ", (e)-[{w: 2.0}]->(f)" +
            ", (e)-[{w: 3.0}]->(g)" +
            ", (f)-[{w: 2.0}]->(g)" +
            ", (f)-[{w: 1.0}]->(h)" +
            ", (g)-[{w: 2.0}]->(h)" +
            // new relationship as a result from mutate
            ", (c)-[{w: 5.0}]->(h)" +
            ", (c)-[{w: 7.0}]->(h)" +
            ", (c)-[{w: 8.0}]->(h)" +
            ""
        );

        assertGraphEquals(expected, actual);
    }
}
