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
import org.neo4j.gds.api.User;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GraphStoreCatalogBusinessFacadePreConditionsDecoratorTest {
    @Test
    void shouldEnsurePreConditionsAreChecked() {
        var delegate = mock(GraphStoreCatalogBusinessFacade.class);
        var preconditionsService = mock(PreconditionsService.class);
        var facade = new GraphStoreCatalogBusinessFacadePreConditionsDecorator(delegate, preconditionsService);

        doThrow(new IllegalStateException("call blocked because reasons"))
            .when(preconditionsService).checkPreconditions();
        assertThatThrownBy(
            () -> facade.graphExists(new User("someUser", false), DatabaseId.from("someDatabase"), "someGraph")
        ).hasMessage("call blocked because reasons");
    }

    @Test
    void shouldNotBlockLegitimateUsage() {
        var delegate = mock(GraphStoreCatalogBusinessFacade.class);
        var preconditionsService = mock(PreconditionsService.class);
        var facade = new GraphStoreCatalogBusinessFacadePreConditionsDecorator(delegate, preconditionsService);

        when(delegate.graphExists(
            new User("someUser", false),
            DatabaseId.from("someDatabase"),
            "someGraph"
        )).thenReturn(true);
        var exists = facade.graphExists(new User("someUser", false), DatabaseId.from("someDatabase"), "someGraph");

        assertTrue(exists);
    }
}
