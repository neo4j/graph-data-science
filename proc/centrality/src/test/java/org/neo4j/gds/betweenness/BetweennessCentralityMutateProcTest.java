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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BetweennessCentralityMutateProcTest
    extends BetweennessCentralityProcTest<BetweennessCentralityMutateProc.MutateResult, BetweennessCentralityMutateConfig>
    implements MutateNodePropertyTest<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityMutateProc.MutateResult, BetweennessCentralityMutateConfig> {

    @Override
    public String mutateProperty() {
        return DEFAULT_RESULT_PROPERTY;
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE;
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
    public Class<? extends AlgoBaseProc<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityMutateProc.MutateResult, BetweennessCentralityMutateConfig>> getProcedureClazz() {
        return BetweennessCentralityMutateProc.class;
    }

    @Override
    public BetweennessCentralityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return BetweennessCentralityMutateConfig.of("",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", DEFAULT_RESULT_PROPERTY);
        }
        return mapWrapper;
    }

    @Test
    void testMutate() {
        String query = GdsCypher
            .call()
            .explicitCreation(BC_GRAPH_NAME)
            .algo("betweenness")
            .mutateMode()
            .addParameter("mutateProperty", DEFAULT_RESULT_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertEquals(0.0, centralityDistribution.get("min"));
            assertEquals(4.0, (double) centralityDistribution.get("max"), 1e-4);

            assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("postProcessingMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("mutateMillis").longValue()));
            assertEquals(5L, row.getNumber("nodePropertiesWritten"));
        });
    }
}
