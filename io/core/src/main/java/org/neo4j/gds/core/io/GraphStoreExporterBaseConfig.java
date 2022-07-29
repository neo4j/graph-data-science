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
package org.neo4j.gds.core.io;

import org.immutables.value.Value;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.ParallelUtil;

public interface GraphStoreExporterBaseConfig extends BaseConfig {

    @Configuration.Ignore
    @Value.Default
    default boolean includeMetaData() {
        return false;
    }

    @Value.Default
    default String defaultRelationshipType() {
        return RelationshipType.ALL_RELATIONSHIPS.name;
    }

    @Value.Default
    default int writeConcurrency() {
        return ConcurrencyConfig.DEFAULT_CONCURRENCY;
    }

    @Value.Default
    default int batchSize() {
        return ParallelUtil.DEFAULT_BATCH_SIZE;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.ConvertWith("org.neo4j.gds.AbstractPropertyMappings#fromObject")
    default org.neo4j.gds.PropertyMappings additionalNodeProperties() {
        return org.neo4j.gds.PropertyMappings.of();
    }
}
