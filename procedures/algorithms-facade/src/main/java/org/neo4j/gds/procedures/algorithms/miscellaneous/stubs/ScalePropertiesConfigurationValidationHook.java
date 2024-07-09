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
package org.neo4j.gds.procedures.algorithms.miscellaneous.stubs;

import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationValidationHook;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;
import org.neo4j.gds.scaling.L1Norm;
import org.neo4j.gds.scaling.L2Norm;
import org.neo4j.gds.scaling.ScalerFactory;

class ScalePropertiesConfigurationValidationHook<CONFIGURATION extends ScalePropertiesBaseConfig> implements ConfigurationValidationHook<CONFIGURATION> {
    private final boolean allowL1L2;

    ScalePropertiesConfigurationValidationHook(boolean allowL1L2) {
        this.allowL1L2 = allowL1L2;
    }

    @Override
    public void onConfigurationParsed(CONFIGURATION configuration) {
        if (!allowL1L2) {
            var specifiedScaler = configuration.scaler().type();
            if ((specifiedScaler.equals(L1Norm.TYPE) || specifiedScaler.equals(L2Norm.TYPE))) {
                ScalerFactory.throwForInvalidScaler(specifiedScaler);
            }
        }
    }
}
