/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.louvain.LouvainFactory;
import org.neo4j.graphalgo.louvain.LouvainProc;
import org.neo4j.graphalgo.louvain.LouvainWriteProc;
import org.neo4j.graphalgo.newapi.LouvainConfigBase;
import org.neo4j.graphalgo.newapi.LouvainWriteConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunities;
import static org.neo4j.graphalgo.louvain.LouvainProc.INCLUDE_INTERMEDIATE_COMMUNITIES_DEFAULT;
import static org.neo4j.graphalgo.louvain.LouvainProc.INCLUDE_INTERMEDIATE_COMMUNITIES_KEY;
import static org.neo4j.graphalgo.louvain.LouvainProc.INNER_ITERATIONS_DEFAULT;
import static org.neo4j.graphalgo.louvain.LouvainProc.INNER_ITERATIONS_KEY;
import static org.neo4j.graphalgo.louvain.LouvainProc.LEVELS_DEFAULT;
import static org.neo4j.graphalgo.louvain.LouvainProc.LEVELS_KEY;
import static org.neo4j.graphalgo.ThrowableRootCauseMatcher.rootCause;
import static org.neo4j.graphalgo.core.ProcedureConstants.DEPRECATED_RELATIONSHIP_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.GRAPH_IMPL_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.SEED_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_KEY;

class LouvainWriteProcTest extends LouvainProcTestBase implements
    WriteConfigTests<LouvainWriteConfig>,
    BaseProcWriteTests<LouvainWriteProc, Louvain, LouvainWriteConfig> {

    @ParameterizedTest(name = "{1}")
    @MethodSource("graphVariations")
    void testWrite(String graphSnippet, String testCaseName) {
        String writeProperty = "myFancyCommunity";
        String query = "CALL gds.algo.louvain.write(" +
                       graphSnippet +
                       "    writeProperty: $writeProp" +
                       "}) YIELD communityCount, modularity, modularities, ranLevels, includeIntermediateCommunities, createMillis, computeMillis, writeMillis, postProcessingMillis, communityDistribution";

        runQuery(query, MapUtil.map("writeProp", writeProperty),
            row -> {
                long communityCount = row.getNumber("communityCount").longValue();
                double modularity = row.getNumber("modularity").doubleValue();
                List<Double> modularities = (List<Double>) row.get("modularities");
                long levels = row.getNumber("ranLevels").longValue();
                boolean includeIntermediate = row.getBoolean("includeIntermediateCommunities");
                long createMillis = row.getNumber("createMillis").longValue();
                long computeMillis = row.getNumber("computeMillis").longValue();
                long writeMillis = row.getNumber("writeMillis").longValue();

                assertEquals(3, communityCount, "wrong community count");
                assertEquals(2, modularities.size(), "invalud modularities");
                assertEquals(2, levels, "invalid level count");
                assertFalse(includeIntermediate, "invalid level count");
                assertTrue(modularity > 0, "wrong modularity value");
                assertTrue(createMillis >= 0, "invalid loadTime");
                assertTrue(writeMillis >= 0, "invalid writeTime");
                assertTrue(computeMillis >= 0, "invalid computeTime");
            }
        );
        assertWriteResult(RESULT, writeProperty);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("graphVariations")
    void testWriteIntermediateCommunities(String graphSnippet, String testCaseName) {
        String writeProperty = "myFancyCommunity";
        String query = "CALL gds.algo.louvain.write(" +
                       graphSnippet +
                       "    writeProperty: $writeProp," +
                       "    includeIntermediateCommunities: true" +
                       "}) YIELD includeIntermediateCommunities";

        runQuery(query, MapUtil.map("writeProp", writeProperty),
            row -> {
                assertTrue(row.getBoolean(INCLUDE_INTERMEDIATE_COMMUNITIES_KEY));
            }
        );

        runQuery(String.format("MATCH (n) RETURN n.%s as %s", writeProperty, writeProperty), row -> {
            Object maybeList = row.get(writeProperty);
            assertTrue(maybeList instanceof long[]);
            long[] communities = (long[]) maybeList;
            assertEquals(2, communities.length);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "writeProperty: null,",
        "writeProperty: '',",
    })
    void testWriteRequiresWritePropertyToBeSet(String writePropertyParameter) {
        String query = "CALL gds.algo.louvain.write({" +
                       writePropertyParameter +
                       "    nodeProjection: ['Node']," +
                       "    relationshipProjection: {" +
                       "      TYPE: {" +
                       "        type: 'TYPE'," +
                       "        projection: 'UNDIRECTED'" +
                       "      }" +
                       "    }" +
                       "}) YIELD includeIntermediateCommunities";

        QueryExecutionException exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(query)
        );

        assertThat(exception, rootCause(
            IllegalArgumentException.class,
            "No value specified for the mandatory configuration parameter `writeProperty`"
        ));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("graphVariations")
    void testWriteWithSeeding(String graphSnippet, String testCaseName) {
        String writeProperty = "myFancyWriteProperty";
        String query = "CALL gds.algo.louvain.write(" +
                       graphSnippet +
                       "    writeProperty: '" + writeProperty + "'," +
                       "    seedProperty: 'seed'" +
                       "}) YIELD communityCount, ranLevels";

        runQuery(
            query,
            row -> {
                assertEquals(3, row.getNumber("communityCount").longValue(), "wrong community count");
                assertEquals(1, row.getNumber("ranLevels").longValue(), "wrong number of levels");
            }
        );
        assertWriteResult(RESULT, writeProperty);
    }

    @Test
    void testCreateConfigWithDefaults() {
        LouvainConfigBase louvainConfig = LouvainWriteConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            CypherMapWrapper.create(MapUtil.map("writeProperty", "writeProperty"))
        );
        assertEquals(false, louvainConfig.includeIntermediateCommunities());
        assertEquals(10, louvainConfig.maxLevels());
    }

    @Override
    public LouvainWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainWriteConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public LouvainWriteProc createProcedure() {
        return new LouvainWriteProc();
    }

    @AllGraphNamesTest
    void testDefaults(String graphImpl) {
        getAlgorithmFactory(
            LouvainProc.class,
            db,
            "", "",
            MapUtil.map(GRAPH_IMPL_KEY, graphImpl),
            (LouvainFactory factory) -> {
                assertEquals(INCLUDE_INTERMEDIATE_COMMUNITIES_DEFAULT, factory.config.includeIntermediateCommunities);
                assertEquals(LEVELS_DEFAULT, factory.config.maxLevel);
                assertEquals(INNER_ITERATIONS_DEFAULT, factory.config.maxInnerIterations);
                assertEquals(TOLERANCE_DEFAULT, factory.config.tolerance);
                assertFalse(factory.config.maybeSeedPropertyKey.isPresent());
            }
        );
    }

    @AllGraphNamesTest
    void testOverwritingDefaults(String graphImpl) {
        Map<String, Object> config = MapUtil.map(
            GRAPH_IMPL_KEY, graphImpl,
            INCLUDE_INTERMEDIATE_COMMUNITIES_KEY, true,
            LEVELS_KEY, 42,
            INNER_ITERATIONS_KEY, 42,
            TOLERANCE_KEY, 0.42,
            SEED_PROPERTY_KEY, "foobar"
        );

        getAlgorithmFactory(
            LouvainProc.class,
            db,
            "", "",
            config,
            (LouvainFactory factory) -> {
                assertTrue(factory.config.includeIntermediateCommunities);
                assertEquals(42, factory.config.maxLevel);
                assertEquals(42, factory.config.maxInnerIterations);
                assertEquals(0.42, factory.config.tolerance);
                assertEquals("foobar", factory.config.maybeSeedPropertyKey.orElse("not_set"));
            }
        );
    }

    @AllGraphNamesTest
    void testGraphLoaderDefaults(String graphImpl) {
        getGraphSetup(
            LouvainProc.class,
            db,
            "", "",
            MapUtil.map(
                GRAPH_IMPL_KEY, graphImpl
            ),
            setup -> {
                assertTrue(setup.loadAnyLabel());
                assertEquals(Direction.BOTH, setup.direction());
                assertFalse(setup.nodePropertyMappings().head().isPresent());
                assertFalse(setup.relationshipPropertyMappings().head().isPresent());
            }
        );
    }

    @AllGraphNamesTest
    void testGraphLoaderWithSeeding(String graphImpl) {
        getGraphSetup(
            LouvainProc.class,
            db,
            "", "",
            MapUtil.map(
                GRAPH_IMPL_KEY, graphImpl,
                SEED_PROPERTY_KEY, "foobar"
            ),
            setup -> {
                PropertyMapping propertyMapping = setup.nodePropertyMappings().head().get();
                assertEquals("foobar", propertyMapping.neoPropertyKey());
                assertEquals("foobar", propertyMapping.propertyKey());
            }
        );
    }

    @AllGraphNamesTest
    void testGraphLoaderWithWeight(String graphImpl) {
        getGraphSetup(
            LouvainProc.class,
            db,
            "", "",
            MapUtil.map(
                GRAPH_IMPL_KEY, graphImpl,
                DEPRECATED_RELATIONSHIP_PROPERTY_KEY, "foobar"
            ),
            setup -> {
                PropertyMapping propertyMapping = setup.relationshipPropertyMappings().head().get();
                assertEquals("foobar", propertyMapping.neoPropertyKey());
                assertEquals("foobar", propertyMapping.propertyKey());
            }
        );
    }

    private void assertWriteResult(List<List<Long>> expectedCommunities, String writeProperty) {
        List<Long> actualCommunities = new ArrayList<>();
        runQuery(String.format("MATCH (n) RETURN id(n) as id, n.%s as community", writeProperty), (row) -> {
            long community = row.getNumber("community").longValue();
            int id = row.getNumber("id").intValue();
            actualCommunities.add(id, community);
        });

        assertCommunities(actualCommunities, expectedCommunities);
    }
}
