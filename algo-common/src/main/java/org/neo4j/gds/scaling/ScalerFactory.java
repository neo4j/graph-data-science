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
package org.neo4j.gds.scaling;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;

public interface ScalerFactory {
    String SCALER_KEY = "type";

    Map<String, Function<CypherMapWrapper, ScalerFactory>> ALL_SCALERS = Map.of(
        NoneScaler.TYPE, NoneScaler::buildFrom,
        Mean.TYPE, Mean::buildFrom,
        Max.TYPE, Max::buildFrom,
        LogScaler.TYPE, LogScaler::buildFrom,
        Center.TYPE, Center::buildFrom,
        StdScore.TYPE, StdScore::buildFrom,
        L1Norm.TYPE, L1Norm::buildFrom,
        L2Norm.TYPE, L2Norm::buildFrom,
        MinMax.TYPE, MinMax::buildFrom
    );

    List<String> SUPPORTED_SCALER_NAMES = ALL_SCALERS
        .keySet()
        .stream()
        .filter(s -> !(s.equals(L1Norm.TYPE) || s.equals(L2Norm.TYPE)))
        .collect(Collectors.toList());

    static String toString(ScalerFactory factory) {
        return factory.type().toUpperCase(Locale.ENGLISH);
    }

    static ScalerFactory parse(Object userInput) {
        if (userInput instanceof ScalerFactory) {
            return (ScalerFactory) userInput;
        }
        if (userInput instanceof String) {
            return parse(Map.of(SCALER_KEY, ((String) userInput)));
        }
        if (userInput instanceof Map) {
            var inputMap = (Map<String, Object>) userInput;
            var scalerSpec = inputMap.get(SCALER_KEY);
            if (scalerSpec instanceof String) {
                var scalerType = toLowerCaseWithLocale((String) scalerSpec);
                var selectedScaler = ALL_SCALERS.get(scalerType);
                if (selectedScaler == null) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Unrecognised scaler type specified: `%s`. Expected one of: %s.",
                        scalerSpec,
                        StringJoining.join(SUPPORTED_SCALER_NAMES)
                    ));
                }
                return selectedScaler.apply(CypherMapWrapper.create(inputMap).withoutEntry(SCALER_KEY));
            }
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Unrecognised scaler type specified: `%s`. Expected one of: %s.",
            userInput,
            StringJoining.join(SUPPORTED_SCALER_NAMES)
        ));
    }

    String type();

    ScalarScaler create(
        NodePropertyValues properties,
        long nodeCount,
        int concurrency,
        ProgressTracker progressTracker,
        ExecutorService executor
    );
}
