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
package org.neo4j.gds.catalog;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.BaseConfig;

import java.util.Optional;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface GraphDropConfig extends BaseConfig {

    @Value.Default
    default boolean failIfMissing() {
        return true;
    }

    Optional<String> databaseName();

    default Optional<String> catalogUser() {
        return this.usernameOverride();
    }

    static GraphDropConfig of(
        boolean failIfMissing,
        String database,
        String username
    ) {
        return ImmutableGraphDropConfig.builder()
            .failIfMissing(failIfMissing)
            .databaseName(StringUtils.trimToNull(database))
            .usernameOverride(StringUtils.trimToNull(username))
            .build();
    }
}
