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
package org.neo4j.graphalgo.louvain;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class LouvainMutateProcTest extends LouvainProcTest<LouvainMutateConfig> implements GraphMutationTest<LouvainMutateConfig, Louvain> {

    private static final String WRITE_PROPERTY = "communityId";

    @Override
    public Optional<String> mutateGraphName() {
        return Optional.of(LOUVAIN_GRAPH);
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a { communityId: 14, seed: 1 })" +
            ", (b { communityId: 14, seed: 1 })" +
            ", (c { communityId: 14, seed: 1 })" +
            ", (d { communityId: 14, seed: 1 })" +
            ", (e { communityId: 14, seed: 1 })" +
            ", (f { communityId: 14, seed: 1 })" +
            ", (g { communityId: 7, seed: 2 })" +
            ", (h { communityId: 7, seed: 2 })" +
            ", (i { communityId: 7, seed: 2 })" +
            ", (j { communityId: 12, seed: 42 })" +
            ", (k { communityId: 12, seed: 42 })" +
            ", (l { communityId: 12, seed: 42 })" +
            ", (m { communityId: 12, seed: 42 })" +
            ", (n { communityId: 12, seed: 42 })" +
            ", (x { communityId: 14, seed: 1 })" +
            // 'LOUVAIN_GRAPH' is UNDIRECTED, e.g. each rel twice
            ", (a)-->(b)-->(a)" +
            ", (a)-->(d)-->(a)" +
            ", (a)-->(f)-->(a)" +
            ", (b)-->(d)-->(b)" +
            ", (b)-->(x)-->(b)" +
            ", (b)-->(g)-->(b)" +
            ", (b)-->(e)-->(b)" +
            ", (c)-->(x)-->(c)" +
            ", (c)-->(f)-->(c)" +
            ", (d)-->(k)-->(d)" +
            ", (e)-->(x)-->(e)" +
            ", (e)-->(f)-->(e)" +
            ", (e)-->(h)-->(e)" +
            ", (f)-->(g)-->(f)" +
            ", (g)-->(h)-->(g)" +
            ", (h)-->(i)-->(h)" +
            ", (h)-->(j)-->(h)" +
            ", (i)-->(k)-->(i)" +
            ", (j)-->(k)-->(j)" +
            ", (j)-->(m)-->(j)" +
            ", (j)-->(n)-->(j)" +
            ", (k)-->(m)-->(k)" +
            ", (k)-->(l)-->(k)" +
            ", (l)-->(n)-->(l)" +
            ", (m)-->(n)-->(m)";
    }

    @Override
    public String failOnExistingTokenMessage() {
        return String.format(
            "Node property `%s` already exists in the in-memory graph.",
            WRITE_PROPERTY
        );
    }

    @Override
    public Class<? extends AlgoBaseProc<?, Louvain, LouvainMutateConfig>> getProcedureClazz() {
        return LouvainMutateProc.class;
    }

    @Override
    public LouvainMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("writeProperty", WRITE_PROPERTY);
        }
        return mapWrapper;
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("louvain")
            .mutateMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .yields(
                "createMillis",
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
                assertNotEquals(-1L, row.getNumber("createMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("mutateMillis"));

                assertEquals(1L, row.get("ranLevels"));
                assertEquals(4L, row.getNumber("communityCount"));
                assertEquals(0.3744, ((List<Double>) row.get("modularities")).get(0), 1E-3);

                assertEquals(MapUtil.map(
                    "p99", 8L,
                    "min", 2L,
                    "max", 8L,
                    "mean", 3.75D,
                    "p90", 8L,
                    "p50", 2L,
                    "p999", 8L,
                    "p95", 8L,
                    "p75", 3L
                ), row.get("communityDistribution"));
            }
        );
    }

}
