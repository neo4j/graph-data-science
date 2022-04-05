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
package org.neo4j.gds.ml.models.automl;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.models.automl.hyperparameter.DoubleRangeParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerRangeParameter;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ParameterParserTest {

    @Test
    void parsesMixedTypes() {
        var userInput = Map.<String, Object>of("maxDepth", Map.of("range", List.of(1.0,2L)));
        var rangeParameters = ParameterParser.parseRangeParameters(userInput);
        assertThat(rangeParameters.doubleRanges().values())
            .usingRecursiveFieldByFieldElementComparator()
            .containsExactlyInAnyOrder(DoubleRangeParameter.of(1.0, 2.0));
    }

    @Test
    void parsesMixedTypesInt() {
        var userInput = Map.<String, Object>of("maxDepth", Map.of("range", List.of(1,2L)));
        var rangeParameters = ParameterParser.parseRangeParameters(userInput);
        assertThat(rangeParameters.integerRanges().values())
            .usingRecursiveFieldByFieldElementComparator()
            .containsExactlyInAnyOrder(IntegerRangeParameter.of(1, 2));
    }

    @Test
    void parsesConcreteAndTunable() {
        var userInput = Map.of("maxDepth", Map.of("range", List.of(1.0,2L)), "maxEpochs", 5);
        var rangeParameters = ParameterParser.parseRangeParameters(userInput);
        assertThat(rangeParameters.doubleRanges().values())
            .usingRecursiveFieldByFieldElementComparator()
            .containsExactlyInAnyOrder(DoubleRangeParameter.of(1.0, 2.0));
        assertThat(ParameterParser.parseConcreteParameters(userInput).values())
            .usingRecursiveFieldByFieldElementComparator()
            .containsExactlyInAnyOrder(IntegerParameter.of(5));
    }
}
