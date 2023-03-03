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
package org.neo4j.gds.degree;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;


class DegreeCentralityMutateProcTest extends DegreeCentralityProcTest<DegreeCentralityMutateConfig>
    implements MutateNodePropertyTest<DegreeCentrality, DegreeCentralityMutateConfig, DegreeCentrality.DegreeFunction> {

    @Override
    public Class<? extends AlgoBaseProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityMutateConfig, ?>> getProcedureClazz() {
        return DegreeCentralityMutateProc.class;
    }

    @Override
    public DegreeCentralityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return DegreeCentralityMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", DEFAULT_RESULT_PROPERTY);
        }
        return mapWrapper;
    }

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
               "  (a { degreeScore: 0.0})" +
               ", (b { degreeScore: 1.0})" +
               ", (c { degreeScore: 1.0})" +
               ", (d { degreeScore: 2.0})" +
               ", (e { degreeScore: 3.0})" +
               ", (f { degreeScore: 2.0})" +
               ", (g { degreeScore: 0.0})" +
               ", (h { degreeScore: 0.0})" +
               ", (i { degreeScore: 0.0})" +
               ", (j { degreeScore: 0.0})" +

               ", (b)-[{weight: 1.0}]->(c)" +

               ", (c)-[{weight: 1.0}]->(b)" +

               ", (d)-[{weight: 1.0}]->(a)" +
               ", (d)-[{weight: 1.0}]->(b)" +

               ", (e)-[{weight: 1.0}]->(b)" +
               ", (e)-[{weight: 1.0}]->(d)" +
               ", (e)-[{weight: 1.0}]->(f)" +

               ", (f)-[{weight: 1.0}]->(b)" +
               ", (f)-[{weight: 1.0}]->(e)";
    }

    @Test
    void testMutate() {
        String query = GdsCypher
            .call(GRAPH_NAME)
            .algo("degree")
            .mutateMode()
            .addParameter("mutateProperty", DEFAULT_RESULT_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("centralityDistribution"))
                .isNotNull()
                .isInstanceOf(Map.class)
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .containsEntry("min", 0.0)
                .hasEntrySatisfying("max",
                    value -> assertThat(value)
                        .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                        .isEqualTo(3.0, Offset.offset(1e-4))
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
                .isEqualTo(10L);
        });
    }
}
