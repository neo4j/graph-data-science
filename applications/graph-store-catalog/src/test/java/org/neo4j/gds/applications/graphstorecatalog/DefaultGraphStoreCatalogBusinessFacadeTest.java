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
package org.neo4j.gds.applications.graphstorecatalog;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.loading.ConfigurationService;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultGraphStoreCatalogBusinessFacadeTest {
    @Test
    void shouldDetermineGraphExists() {
        var service = mock(GraphStoreCatalogService.class);
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            null,
            new GraphNameValidationService(),
            service,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        when(service.graphExists(
            new User("someUser", false),
            DatabaseId.from("someDatabase"),
            GraphName.parse("someGraph")
        )).thenReturn(true);
        var graphExists = facade.graphExists(new User("someUser", false), DatabaseId.from("someDatabase"), "someGraph");

        assertTrue(graphExists);
    }

    @Test
    void shouldDetermineGraphDoesNotExist() {
        var service = mock(GraphStoreCatalogService.class);
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            null,
            mock(GraphNameValidationService.class),
            service,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        when(service.graphExists(
            new User("someUser", false),
            DatabaseId.from("someDatabase"),
            GraphName.parse("someGraph")
        )).thenReturn(false);
        boolean graphExists = facade.graphExists(
            new User("someUser", false),
            DatabaseId.from("someDatabase"),
            "someGraph"
        );

        assertFalse(graphExists);
    }

    @Test
    void shouldValidateInputGraphName() {
        var service = mock(GraphStoreCatalogService.class);
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            null,
            new GraphNameValidationService(),
            service,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertThatThrownBy(
            () -> facade.graphExists(new User("someUser", false), DatabaseId.from("someDatabase"), "   ")
        ).hasMessage("`graphName` can not be null or blank, but it was `   `");
    }

    @Test
    void shouldUseStrictValidationWhenProjecting() {
        var validationService = mock(GraphNameValidationService.class);
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            null,
            validationService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        when(validationService.validateStrictly("   some graph   "))
            .thenThrow(new IllegalArgumentException("whitespace!"));

        assertThatIllegalArgumentException().isThrownBy(
            () -> facade.nativeProject(
                null,
                null,
                null,
                null,
                null,
                null,
                "   some graph   ",
                null,
                null,
                null
            )
        ).withMessage("whitespace!");

        assertThatIllegalArgumentException().isThrownBy(
            () -> facade.cypherProject(
                null,
                null,
                null,
                null,
                null,
                null,
                "   some graph   ",
                null,
                null,
                null
            )
        ).withMessage("whitespace!");
    }

    /**
     * This is a port of an old thing from the procedure level test
     */
    @Test
    void shouldHandleNullsInNativeProjectParameters() {
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            new ConfigurationService(),
            new GraphNameValidationService(),
            mock(GraphStoreCatalogService.class),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertThatIllegalArgumentException().isThrownBy(() -> facade.nativeProject(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )).withMessage("`graphName` can not be null or blank, but it was `null`");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.nativeProject(
            new User("some user", false),
            null,
            null,
            null,
            null,
            null,
            "some name",
            null,
            null,
            null
        )).withMessage("Multiple errors in configuration arguments:\n" +
            "\t\t\t\tNo value specified for the mandatory configuration parameter `nodeProjection`\n" +
            "\t\t\t\tNo value specified for the mandatory configuration parameter `relationshipProjection`");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.nativeProject(
            new User("some user", false),
            null,
            null,
            null,
            null,
            null,
            "some name",
            "some node projection",
            null,
            null
        )).withMessage("No value specified for the mandatory configuration parameter `relationshipProjection`");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.nativeProject(
            new User("some user", false),
            null,
            null,
            null,
            null,
            null,
            "some name",
            null,
            "some relationship projection",
            null
        )).withMessage("No value specified for the mandatory configuration parameter `nodeProjection`");
    }

    /**
     * This is a port of an old thing from the procedure level test
     */
    @Test
    void shouldHandleNullsInCypherProjectParameters() {
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            new ConfigurationService(),
            new GraphNameValidationService(),
            mock(GraphStoreCatalogService.class),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertThatIllegalArgumentException().isThrownBy(() -> facade.cypherProject(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )).withMessage("`graphName` can not be null or blank, but it was `null`");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.cypherProject(
            new User("some user", false),
            null,
            null,
            null,
            null,
            null,
            "some name",
            null,
            null,
            null
        )).withMessage("Multiple errors in configuration arguments:\n" +
            "\t\t\t\tNo value specified for the mandatory configuration parameter `nodeQuery`\n" +
            "\t\t\t\tNo value specified for the mandatory configuration parameter `relationshipQuery`");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.cypherProject(
            new User("some user", false),
            null,
            null,
            null,
            null,
            null,
            "some name",
            "some node projection",
            null,
            null
        )).withMessage("No value specified for the mandatory configuration parameter `relationshipQuery`");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.cypherProject(
            new User("some user", false),
            null,
            null,
            null,
            null,
            null,
            "some name",
            null,
            "some relationship projection",
            null
        )).withMessage("No value specified for the mandatory configuration parameter `nodeQuery`");
    }

    @Test
    void shouldDoExistenceCheckWhenProjecting() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            null,
            new GraphNameValidationService(),
            graphStoreCatalogService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var user = new User("some user", false);
        var databaseId = DatabaseId.from("some database name");
        doThrow(new IllegalArgumentException("it's alive?!")).when(graphStoreCatalogService).ensureGraphDoesNotExist(
            user,
            databaseId,
            GraphName.parse("some existing graph")
        );

        assertThatIllegalArgumentException().isThrownBy(() -> facade.nativeProject(
            user,
            databaseId,
            null,
            null,
            null,
            null,
            "some existing graph",
            "some node projection",
            "some relationship projection",
            null
        )).withMessage("it's alive?!");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.cypherProject(
            user,
            databaseId,
            null,
            null,
            null,
            null,
            "some existing graph",
            "some node query",
            "some relationship query",
            null
        )).withMessage("it's alive?!");

        assertThatIllegalArgumentException().isThrownBy(() -> facade.subGraphProject(
            user,
            databaseId,
            null,
            null,
            "some existing graph",
            null,
            null,
            null,
            null
        )).withMessage("it's alive?!");
    }

    @Test
    void shouldDoPositiveExistenceCheckWhenProjectingSubGraph() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var facade = new DefaultGraphStoreCatalogBusinessFacade(
            null,
            new GraphNameValidationService(),
            graphStoreCatalogService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var user = new User("some user", false);
        var databaseId = DatabaseId.from("some database name");
        doThrow(new IllegalArgumentException("Damn you, Jack Sparrow!")).when(graphStoreCatalogService)
            .ensureGraphExists(
                user,
                databaseId,
                GraphName.parse("some non-existing graph")
            );

        assertThatIllegalArgumentException().isThrownBy(() -> facade.subGraphProject(
            user,
            databaseId,
            null,
            null,
            "some existing graph",
            "some non-existing graph",
            null,
            null,
            null
        )).withMessage("Damn you, Jack Sparrow!");
    }
}
