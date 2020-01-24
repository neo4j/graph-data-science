/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.newapi;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.internal.kernel.api.security.AuthSubject;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public interface BaseConfig {
    @Configuration.Parameter
    @Value.Default
    default String username() {
        return AuthSubject.ANONYMOUS.username();
    };

    @Configuration.Ignore
    default Class<? extends GraphFactory> getGraphImpl() {
        return HugeGraphFactory.class;
    };

    @Configuration.CollectKeys
    @Value.Auxiliary
    @Value.Default
    @Value.Parameter(false)
    default Collection<String> configKeys() {
        return Collections.emptyList();
    };

    @Configuration.ToMap
    @Value.Auxiliary
    @Value.Default
    @Value.Parameter(false)
    default Map<String, Object> toMap() {
        return Collections.emptyMap();
    };
}
