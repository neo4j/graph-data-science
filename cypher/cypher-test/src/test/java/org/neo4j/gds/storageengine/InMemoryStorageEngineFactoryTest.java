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
package org.neo4j.gds.storageengine;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.junit.annotation.EnableForNeo4jVersion;
import org.neo4j.storageengine.api.StorageEngineFactory;

import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryStorageEngineFactoryTest {

    @EnableForNeo4jVersion(Neo4jVersion.V_4_4)
    @Test
    void allStorageEnginesHaveUniqueNames() {
        var storageEngines = ServiceLoader
            .load(StorageEngineFactory.class, StorageEngineFactory.class.getClassLoader())
            .stream();

        var names = storageEngines
            .map(factory -> factory.get().name())
            .collect(Collectors.toList());

        assertThat(names).doesNotHaveDuplicates();
    }
}
