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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalogBusinessFacade;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.progress.tasks.LeafTask;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;
import org.neo4j.gds.core.utils.warnings.UserLogStore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.SecurityContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GraphStoreCatalogProcedureFacadeTest {
    @Test
    void shouldStackOffNeo4jThings() {
        var databaseIdService = mock(DatabaseIdService.class);
        var graphDatabaseService = mock(GraphDatabaseService.class);
        var securityContext = mock(SecurityContext.class);
        var usernameService = mock(UserServices.class);
        var businessFacade = mock(GraphStoreCatalogBusinessFacade.class);
        var procedureFacade = new GraphStoreCatalogProcedureFacade(
            databaseIdService,
            graphDatabaseService,
            null,
            null,
            null,
            securityContext,
            null,
            null,
            usernameService,
            businessFacade
        );

        when(usernameService.getUser(securityContext)).thenReturn(new User("current user", false));
        when(databaseIdService.getDatabaseId(graphDatabaseService)).thenReturn(DatabaseId.from("current database"));
        procedureFacade.graphExists("some graph");

        verify(businessFacade).graphExists("current user", DatabaseId.from("current database"), "some graph");
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
        var usernameService = mock(UserServices.class);
        var businessFacade = mock(GraphStoreCatalogBusinessFacade.class);
        var securityContext = mock(SecurityContext.class);
        var procedureFacade = new GraphStoreCatalogProcedureFacade(
            databaseIdService,
            graphDatabaseService,
            null,
            null,
            null,
            securityContext,
            null,
            userLogServices,
            usernameService,
            businessFacade
        );

        var databaseId = DatabaseId.from("current database");
        when(databaseIdService.getDatabaseId(graphDatabaseService)).thenReturn(databaseId);
        var userLogStore = mock(UserLogStore.class);
        when(userLogServices.getUserLogStore(databaseId)).thenReturn(userLogStore);
        when(usernameService.getUser(securityContext)).thenReturn(new User("current user", false));
        var expectedWarnings = Stream.of(
            new UserLogEntry(new LeafTask("lt", 42), "going once"),
            new UserLogEntry(new LeafTask("lt", 87), "going twice..."),
            new UserLogEntry(new LeafTask("lt", 23), "gone!")
        );
        when(userLogStore.query("current user")).thenReturn(expectedWarnings);
        var actualWarnings = procedureFacade.queryUserLog(null);

        assertThat(actualWarnings).isSameAs(expectedWarnings);
    }
}
