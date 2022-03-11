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
package org.neo4j.gds.impl.traverse;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.SourceNodeConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collections;
import java.util.List;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface DfsStreamConfig extends AlgoBaseConfig, SourceNodeConfig, ConcurrencyConfig {

    long NO_MAX_DEPTH = -1L;

    @Value.Default
    default List<Long> targetNodes() {
        return Collections.emptyList();
    }

    @Value.Default
    default long maxDepth() {
        return NO_MAX_DEPTH;
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean hasTargetNodes() {
        return !targetNodes().isEmpty();
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean hasMaxDepth() {
        return maxDepth() != NO_MAX_DEPTH;
    }

    static DfsStreamConfig of(CypherMapWrapper userInput) {
        return new DfsStreamConfigImpl(userInput);
    }
}
