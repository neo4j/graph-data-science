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
package org.neo4j.graphalgo.influenceΜaximization;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;

@Configuration
@ValueClass
@SuppressWarnings("immutables:subtype")
public interface InfluenceMaximizationConfig extends AlgoBaseConfig, WritePropertyConfig { //BaseConfig
    String DEFAULT_TARGET_PROPERTY = "spread";

    @Configuration.IntegerRange(min = 1)
    int k();

    @Value.Default
    @Configuration.DoubleRange(min = 0.01, max = 1)
    default double p() {
        return 0.1;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int mc() {
        return 1000;
    }

    @Override
    default String writeProperty() {
        return DEFAULT_TARGET_PROPERTY;
    }
}
