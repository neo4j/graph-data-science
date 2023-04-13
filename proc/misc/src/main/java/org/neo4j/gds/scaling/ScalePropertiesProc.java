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

import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;
import org.neo4j.gds.utils.StringJoining;

import static org.neo4j.gds.scaling.ScalerFactory.SUPPORTED_SCALER_NAMES;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ScalePropertiesProc {

    private ScalePropertiesProc() {}

    static final String SCALE_PROPERTIES_DESCRIPTION = "Scale node properties";

    static NodePropertyValues nodeProperties(
        ComputationResult<ScaleProperties, ScaleProperties.Result, ? extends ScalePropertiesBaseConfig> computationResult
    ) {
        var size = computationResult.graph().nodeCount();
        var scaledProperties = computationResult.result()
            .map(ScaleProperties.Result::scaledProperties)
            .orElseGet(() -> HugeObjectArray.newArray(double[].class, 0));

        return new DoubleArrayNodePropertyValues() {
            @Override
            public long nodeCount() {
                return size;
            }

            @Override
            public double[] doubleArrayValue(long nodeId) {
                return scaledProperties.get(nodeId);
            }
        };
    }

    static void validateLegacyScalers(ScalePropertiesBaseConfig config, boolean allowL1L2Scalers) {
        if (!allowL1L2Scalers && (config.scaler().type().equals(L1Norm.TYPE) || config.scaler().type().equals(L2Norm.TYPE))) {
            throw new IllegalArgumentException(formatWithLocale(
                "Unrecognised scaler type specified: `%s`. Expected one of: %s.",
                config.scaler().type(),
                StringJoining.join(SUPPORTED_SCALER_NAMES)
            ));
        }
    }
}
