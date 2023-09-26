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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;
import org.neo4j.gds.core.utils.warnings.UserLogStore;
import org.neo4j.gds.procedures.catalog.CatalogFacade;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CatalogFacadeTest {
    @Test
    void shouldStackOffNeo4jThings() {
        var databaseIdService = mock(DatabaseIdService.class);
        var graphDatabaseService = mock(GraphDatabaseService.class);
        var businessFacade = mock(CatalogBusinessFacade.class);
        var catalogFacade = new CatalogFacade(
            databaseIdService,
            graphDatabaseService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new User("current user", false),
            businessFacade
        );

        when(databaseIdService.getDatabaseId(graphDatabaseService)).thenReturn(DatabaseId.of("current database"));
        catalogFacade.graphExists("some graph");

        verify(businessFacade).graphExists(
            new User("current user", false),
            DatabaseId.of("current database"),
            "some graph"
        );
    }

    /**
     * This is a lot of dependency mocking, sure; but that is the nature of this facade, it interacts with Neo4j's
     * integration points and distills domain concepts: database and user. It then uses those to interact with domain
     * services, the user log in this case.
     * <p>
     * How many of these will we need to gain confidence?
     * Might we engineer some reuse or commonality out so that we can test this once but have it apply across the board?
     * Sure! That's just us engineering.
     */
    @Test
    void shouldQueryUserLog() {
        var databaseIdService = mock(DatabaseIdService.class);
        var graphDatabaseService = mock(GraphDatabaseService.class);
        var userLogServices = mock(UserLogServices.class);
        var businessFacade = mock(CatalogBusinessFacade.class);
        var catalogFacade = new CatalogFacade(
            databaseIdService,
            graphDatabaseService,
            null,
            null,
            null,
            null,
            null,
            null,
            userLogServices,
            new User("current user", false),
            businessFacade
        );

        var databaseId = DatabaseId.of("current database");
        when(databaseIdService.getDatabaseId(graphDatabaseService)).thenReturn(databaseId);
        var userLogStore = mock(UserLogStore.class);
        when(userLogServices.getUserLogStore(databaseId)).thenReturn(userLogStore);
        var expectedWarnings = Stream.of(
            new UserLogEntry(new LeafTask("lt", 42), "going once"),
            new UserLogEntry(new LeafTask("lt", 87), "going twice..."),
            new UserLogEntry(new LeafTask("lt", 23), "gone!")
        );
        when(userLogStore.query("current user")).thenReturn(expectedWarnings);
        var actualWarnings = catalogFacade.queryUserLog(null);

        assertThat(actualWarnings).isSameAs(expectedWarnings);
    }
}
