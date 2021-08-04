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
package org.neo4j.gds.ml.linkmodels.pipeline;


import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;

import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface LinkPredictionSplitConfig extends Model.Mappable {

    @Configuration.IntegerRange(min = 2)
    default int validationFolds() {
        return 3;
    };

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double testFraction() {
        return 0.1;
    };

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double trainFraction() {
        return 0.1;
    };

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double negativeSamplingRatio() {
        return 1.0;
    };

    static LinkPredictionSplitConfig of(CypherMapWrapper config) {
        return new LinkPredictionSplitConfigImpl(config);
    }

    @Override
    @Configuration.ToMap
    Map<String, Object> toMap();

    @Value.Check
    default void validFractionSum() {
        var fractionSum = testFraction() + trainFraction();
        if (fractionSum > 1.0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Sum of fractions for test and train set must be not greater than 1.0. But got %s.",
                fractionSum
            ));
        }
    }
}
