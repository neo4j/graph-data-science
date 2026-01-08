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
package org.neo4j.gds.core;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.projection.GraphStoreFactorySupplier;
import org.neo4j.gds.projection.GraphStoreFactorySuppliers;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

class GraphStoreFactorySuppliersTest {
    @Test
    void shouldSupplyGraphStoreFactory() {
        var supplier = mock(GraphStoreFactorySupplier.class);
        var suppliers = new GraphStoreFactorySuppliers(Map.of(SomeGraphProjectConfig.class, cfg -> supplier));

        var actual = suppliers.find(new SomeGraphProjectConfig());

        assertEquals(supplier, actual);
    }

    @Test
    void shouldSupplyGraphStoreFactoryEvenForSubClasses() {
        var supplier = mock(GraphStoreFactorySupplier.class);
        var suppliers = new GraphStoreFactorySuppliers(Map.of(GraphProjectConfig.class, cfg -> supplier));

        var actual = suppliers.find(new SomeGraphProjectConfig());

        assertEquals(supplier, actual);
    }

    @Test
    void shouldNotSupplyGraphStoreFactoryWhenNotConfigured() {
        var suppliers = new GraphStoreFactorySuppliers(Collections.emptyMap());

        try {
            suppliers.find(new SomeGraphProjectConfig());

            fail();
        } catch (IllegalStateException e) {
            assertEquals("unable to supply graph store factory for SomeGraphProjectConfig", e.getMessage());
        }
    }
}
