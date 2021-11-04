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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.immutables.value.Value;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;

@ValueClass
@Configuration
public interface NodeClassificationSplitConfig extends ToMapConvertible {
    NodeClassificationSplitConfig DEFAULT_CONFIG = NodeClassificationSplitConfig.of(CypherMapWrapper.empty());

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1)
    default double holdoutFraction() {
        return 0.3;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 2)
    default int validationFolds() {
        return 3;
    }

    static NodeClassificationSplitConfig of(CypherMapWrapper config) {
        return new NodeClassificationSplitConfigImpl(config);
    }

    @TestOnly
    static ImmutableNodeClassificationSplitConfig.Builder builder() {
        return ImmutableNodeClassificationSplitConfig.builder();
    }

    @Override
    @Configuration.ToMap
    Map<String, Object> toMap();
}
