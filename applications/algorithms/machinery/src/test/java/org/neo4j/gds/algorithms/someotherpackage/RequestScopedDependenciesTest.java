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
package org.neo4j.gds.algorithms.someotherpackage;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;

class RequestScopedDependenciesTest {
    @Test
    void shouldBuildBespokeProducts() {
        var rsd = RequestScopedDependencies.builder()
            .databaseId(DatabaseId.of("IMDB"))
            .terminationFlag(TerminationFlag.STOP_RUNNING)
            .user(new User("Colin Needham", true))
            .build();

        assertThat(rsd.databaseId()).isEqualTo(DatabaseId.of("IMDB"));
        assertThat(rsd.terminationFlag()).isEqualTo(TerminationFlag.STOP_RUNNING);
        assertThat(rsd.user()).isEqualTo(new User("Colin Needham", true));
    }
}
