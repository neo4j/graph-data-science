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
package org.neo4j.gds.ml.pipeline;


import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Configuration
public interface AutoTuningConfig extends ToMapConvertible {
    int MAX_TRIALS = 10;

    AutoTuningConfig DEFAULT_CONFIG = AutoTuningConfig.of(CypherMapWrapper.empty());

    @Configuration.IntegerRange(min = 1)
    default int maxTrials() {
        return MAX_TRIALS;
    }

    @Override
    @Configuration.ToMap
    Map<String, Object> toMap();

    @Configuration.CollectKeys
    default Collection<String> configKeys() {
        return Collections.emptyList();
    }

    static AutoTuningConfig of(CypherMapWrapper config) {
        return new AutoTuningConfigImpl(config);
    }
}
