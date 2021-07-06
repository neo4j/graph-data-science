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

import org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions.L2FeatureStep;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.StringFormatting.toUpperCaseWithLocale;

public enum LinkFeatureStepFactory {
    HADAMARD(HadamardFeatureStep::new, HadamardFeatureStep::validateConfig),
    COSINE(CosineFeatureStep::new, CosineFeatureStep::validateConfig),
    L2(L2FeatureStep::new, L2FeatureStep::validateConfig);

    private final Function<Map<String, Object>, LinkFeatureStep> buildFunction;
    private final Validation validation;

    LinkFeatureStepFactory(Function<Map<String, Object>, LinkFeatureStep> buildFunction, Validation validation) {
        this.buildFunction = buildFunction;
        this.validation = validation;
    }

    public static final List<String> VALUES = Arrays
        .stream(LinkFeatureStepFactory.values())
        .map(LinkFeatureStepFactory::name)
        .collect(Collectors.toList());

    private static LinkFeatureStepFactory parse(String input) {
        var inputString = toUpperCaseWithLocale(input);

        if (VALUES.contains(inputString)) {
            return LinkFeatureStepFactory.valueOf(inputString);
        }

        throw new IllegalArgumentException(formatWithLocale(
            "LinkFeatureStepFactory `%s` is not supported. Must be one of: %s.",
            inputString,
            VALUES
        ));
    }

    public static LinkFeatureStep create(String taskName, Map<String, Object> config) {
        LinkFeatureStepFactory factory = parse(taskName);
        factory.validation.validateConfig(config);
        return factory.buildFunction.apply(config);
    }

    interface Validation {
        void validateConfig(Map<String, Object> config);
    }
}
