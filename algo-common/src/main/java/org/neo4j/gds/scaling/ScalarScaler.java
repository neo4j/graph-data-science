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
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.utils.StringJoining;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;

public abstract class ScalarScaler implements Scaler {

    protected final NodePropertyValues properties;

    protected ScalarScaler(NodePropertyValues properties) {this.properties = properties;}

    @Override
    public int dimension() {
        return 1;
    }

    static final ScalarScaler ZERO = new ScalarScaler(null) {
        @Override
        public double scaleProperty(long nodeId) {
            return 0;
        }
    };

    public interface ScalerFactory {
        String SCALER_KEY = "scaler";

        Map<String, Function<CypherMapWrapper, ScalerFactory>> SUPPORTED_SCALERS = Map.of(
            NoneScaler.NAME, NoneScaler::buildFrom,
            Mean.NAME, Mean::buildFrom,
            Max.NAME, Max::buildFrom,
            LogScaler.NAME, LogScaler::buildFrom,
            Center.NAME, Center::buildFrom,
            StdScore.NAME, StdScore::buildFrom,
            L1Norm.NAME, L1Norm::buildFrom,
            L2Norm.NAME, L2Norm::buildFrom,
            MinMax.NAME, MinMax::buildFrom
        );

        static String toString(ScalerFactory factory) {
            return factory.name().toUpperCase(Locale.ENGLISH);
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
                    var scalerName = toLowerCaseWithLocale((String) scalerSpec);
                    var selectedScaler = SUPPORTED_SCALERS.get(scalerName);
                    if (selectedScaler == null) {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Unrecognised scaler specified: `%s`. Expected one of: %s.",
                            scalerSpec,
                            StringJoining.join(SUPPORTED_SCALERS.keySet())
                        ));
                    }
                    return selectedScaler.apply(CypherMapWrapper.create(inputMap).withoutEntry("scaler"));
                }
            }
            throw new IllegalArgumentException(formatWithLocale(
                "Unrecognised scaler specified: `%s`. Expected one of: %s.",
                userInput,
                StringJoining.join(SUPPORTED_SCALERS.keySet())
            ));
        }

        String name();

        ScalarScaler create(
            NodePropertyValues properties,
            long nodeCount,
            int concurrency,
            ExecutorService executor
        );
    }

    abstract static class AggregatesComputer implements Runnable {

        private final Partition partition;
        final NodePropertyValues properties;

        AggregatesComputer(Partition partition, NodePropertyValues property) {
            this.partition = partition;
            this.properties = property;
        }

        @Override
        public void run() {
            long end = partition.startNode() + partition.nodeCount();
            for (long nodeId = partition.startNode(); nodeId < end; nodeId++) {
                compute(nodeId);
            }
        }

        abstract void compute(long nodeId);
    }
}
