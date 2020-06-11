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
package org.neo4j.graphalgo.betweenness;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.MutateNodePropertyTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.values.storable.NumberType;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class BetweennessCentralityMutateProcTest
    extends BetweennessCentralityProcTest<BetweennessCentralityMutateConfig>
    implements MutateNodePropertyTest<BetweennessCentrality, BetweennessCentralityMutateConfig, HugeAtomicDoubleArray> {

    @Override
    public String mutateProperty() {
        return DEFAULT_RESULT_PROPERTY;
    }

    @Override
    public NumberType mutatePropertyType() {
        return NumberType.FLOATING_POINT;
    }

    @Override
    public String expectedMutatedGraph() {
        return "CREATE" +
               "  (a {centrality: 0.0})" +
               ", (b {centrality: 3.0})" +
               ", (c {centrality: 4.0})" +
               ", (d {centrality: 3.0})" +
               ", (e {centrality: 0.0})" +
               ", (a)-->(b)" +
               ", (b)-->(c)" +
               ", (c)-->(d)" +
               ", (d)-->(e)";
    }

    @Override
    public Class<? extends AlgoBaseProc<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityMutateConfig>> getProcedureClazz() {
        return BetweennessCentralityMutateProc.class;
    }

    @Override
    public BetweennessCentralityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return BetweennessCentralityMutateConfig.of("",
            Optional.empty(),
            Optional.empty(),
            mapWrapper.withNumber("probability", DEFAULT_PROBABILITY)
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", DEFAULT_RESULT_PROPERTY);
        }
        if (!mapWrapper.containsKey("probability")) {
            mapWrapper = mapWrapper.withNumber("probability", DEFAULT_PROBABILITY);
        }
        return mapWrapper;
    }

    @Test
    void testMutate() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("betweenness")
            .mutateMode()
            .addParameter("mutateProperty", DEFAULT_RESULT_PROPERTY)
            .addParameter("probability", DEFAULT_PROBABILITY)
            .yields(
                "nodePropertiesWritten",
                "createMillis",
                "computeMillis",
                "mutateMillis",
                "minCentrality",
                "maxCentrality",
                "sumCentrality"
            );

        runQueryWithRowConsumer(query, row -> {
            assertEquals(5L, row.getNumber("nodePropertiesWritten"));

            assertNotEquals(-1L, row.getNumber("createMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("mutateMillis"));

            assertEquals(0D, row.getNumber("minCentrality").doubleValue(), 1E-1);
            assertEquals(4D, row.getNumber("maxCentrality").doubleValue(), 1E-1);
            assertEquals(10D, row.getNumber("sumCentrality").doubleValue(), 1E-1);
        });
    }
}
