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
package org.neo4j.graphalgo.beta.pregel;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.IterationsConfig;
import org.neo4j.graphalgo.config.MutatePropertyConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface PregelConfig extends
    AlgoBaseConfig,
    RelationshipWeightConfig,
    IterationsConfig,
    WritePropertyConfig,
    MutatePropertyConfig,
    ConcurrencyConfig {

    @Value.Default
    default boolean isAsynchronous() {
        return false;
    }

    @Value.Default
    default String writeProperty() {
        return "";
    }

    @Value.Default
    default String mutateProperty() {
        return "";
    }

    @Value.Default
    @Configuration.Key(WRITE_CONCURRENCY_KEY)
    @Override
    default int writeConcurrency() {
        return concurrency();
    }

    @Value.Default
    @Configuration.ConvertWith("org.neo4j.graphalgo.beta.pregel.Partitioning#parse")
    @Configuration.ToMapValue("org.neo4j.graphalgo.beta.pregel.Partitioning#toString")
    default Partitioning partitioning() {
        return Partitioning.RANGE;
    }

    @Value.Derived
    @Configuration.Ignore
    default boolean useForkJoin() {
        return partitioning() == Partitioning.AUTO;
    }

    static PregelConfig of(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new PregelConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }
}
