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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

public class BetweennessCentralityMutateProcTest
    extends BetweennessCentralityProcTest<BetweennessCentralityMutateConfig>
    implements MutateNodePropertyTest<BetweennessCentrality, BetweennessCentralityMutateConfig, HugeAtomicDoubleArray> {

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
    public Class<? extends AlgoBaseProc<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityMutateConfig, ?>> getProcedureClazz() {
        return BetweennessCentralityMutateProc.class;
    }

    @Override
    public BetweennessCentralityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return BetweennessCentralityMutateConfig.of(mapWrapper);
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
            .call(BC_GRAPH_NAME)
            .algo("betweenness")
            .mutateMode()
            .addParameter("mutateProperty", DEFAULT_RESULT_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("centralityDistribution"))
                .isNotNull()
                .isInstanceOf(Map.class)
                .asInstanceOf(MAP)
                .containsEntry("min", 0.0)
                .hasEntrySatisfying("max",
                    value -> assertThat(value)
                        .asInstanceOf(DOUBLE)
                        .isEqualTo(4.0, Offset.offset(1e-4))
                );

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("mutateMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .isEqualTo(5);
        });
    }
}
