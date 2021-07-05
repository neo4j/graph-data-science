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

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.neo4j.graphalgo.utils.StringFormatting.toLowerCaseWithLocale;

public class LinkFeatureStepFactory {
    private static final Map<String, Function<Map<String, Object>, LinkFeatureStep>> MAP = Map.of(
        "hadamard",
        config -> new HadamardFeatureStep((List<String>) config.getOrDefault("featureProperties", null)),
        "cosine",
        config -> new CosineFeatureStep((List<String>) config.getOrDefault("featureProperties", null))
    );

    private LinkFeatureStepFactory() {}

    public static LinkFeatureStep create(String taskName, Map<String, Object> config) {
        var lowerCaseTaskName = toLowerCaseWithLocale(taskName);
        if (MAP.containsKey(lowerCaseTaskName)) {
            return MAP.get(lowerCaseTaskName).apply(config);
        } else {
            throw new UnsupportedOperationException("Could not find that task");
        }
    }
}
