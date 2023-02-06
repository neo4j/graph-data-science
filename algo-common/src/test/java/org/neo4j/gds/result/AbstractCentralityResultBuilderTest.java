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
package org.neo4j.gds.result;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.ProcedureReturnColumns;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractCentralityResultBuilderTest {

    @Test
    void catchHistogramAIOOBbug() {
        var procedureReturnColumns = new ProcedureReturnColumns() {
            @Override
            public boolean contains(String fieldName) {
                return fieldName.equals("centralityDistribution");
            }

            @Override
            public ProcedureReturnColumns withReturnColumnNameTransformationFunction(Function<String, String> transformationFunction) {
                return null;
            }
        };
        AbstractCentralityResultBuilder<Object> builder = new AbstractCentralityResultBuilder<>(procedureReturnColumns, 4) {
            @Override
            protected Object buildResult() {
                return null;
            }
        };

        double[] problematicCentralities = {
            0.1473766911865831,
            0.06643103322406599,
            1.45519152283669E-11,
            1.45984899931293E-77
        };

        builder
            .withCentralityFunction(id -> problematicCentralities[(int) id])
            .withNodeCount(problematicCentralities.length)
            .build();

        assertThat(builder.centralityHistogram).containsOnlyKeys(AbstractCentralityResultBuilder.HISTOGRAM_ERROR_KEY);
    }

}
