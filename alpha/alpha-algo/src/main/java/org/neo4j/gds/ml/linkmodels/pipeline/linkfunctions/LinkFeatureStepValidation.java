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
package org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkFeatureStep.FEATURE_PROPERTIES;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class LinkFeatureStepValidation {
    public static void validateConfig(String taskName, Map<String, Object> config) {
        if (!config.containsKey(FEATURE_PROPERTIES)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Configuration for %s is missing `%s`",
                taskName,
                FEATURE_PROPERTIES
            ));
        }
        var featureProperties = config.get(FEATURE_PROPERTIES);
        if (featureProperties instanceof List) {
            if (((List<?>) featureProperties).isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Configuration for %s requires a non-empty list of strings for `%s`",
                    taskName,
                    FEATURE_PROPERTIES
                ));
            }
            if (((List<?>) featureProperties).stream().allMatch(elem -> elem instanceof String)) {
                return;
            }
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Configuration for %s expects `%s` to be a list of strings",
            taskName,
            FEATURE_PROPERTIES
        ));
    }
}
