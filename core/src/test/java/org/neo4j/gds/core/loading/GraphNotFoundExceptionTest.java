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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;

class GraphNotFoundExceptionTest {

    @Test
    void shouldCreateMessageFromStrings() {
        var exception = new GraphNotFoundException("myGraph", "myDatabase");

        assertThat(exception)
            .isInstanceOf(NoSuchElementException.class)
            .hasMessage("Graph with name `myGraph` does not exist on database `myDatabase`. It might exist on another database.");
        assertThat(exception.graphName()).isEqualTo("myGraph");
        assertThat(exception.databaseName()).isEqualTo("myDatabase");
    }

    @Test
    void shouldCreateMessageFromUserCatalogKey() {
        var userCatalogKey = GraphStoreCatalog.UserCatalogKey.of("myDatabase", "myGraph");

        var exception = new GraphNotFoundException(userCatalogKey);

        assertThat(exception)
            .hasMessage("Graph with name `myGraph` does not exist on database `myDatabase`. It might exist on another database.");
        assertThat(exception.graphName()).isEqualTo("myGraph");
        assertThat(exception.databaseName()).isEqualTo("myDatabase");
    }
}
