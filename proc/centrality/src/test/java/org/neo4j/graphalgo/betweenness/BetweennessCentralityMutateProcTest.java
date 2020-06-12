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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

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
        if (!mapWrapper.containsKey("mutateProperty")) {
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
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "minCentrality", 0.0,
            "maxCentrality", 4.0,
            "sumCentrality", 10.0,
            "nodePropertiesWritten", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }
}
