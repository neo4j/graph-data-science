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
package org.neo4j.graphalgo.core.concurrency;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.compat.GraphDbApi;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrencyControllerExtensionTest {

    private GraphDbApi db;

    @AfterEach
    void reset() {
        db.shutdown();
    }

    @Test
    void shouldSetMonitorFalse() {
        db = TestDatabaseCreator.createTestDatabase();

        assertFalse(ConcurrencyMonitor.instance().isUnlimited());
        assertTrue(ConcurrencyMonitor.instance().isLimited());
    }

    @Test
    void shouldSetMonitorTrue() {
        db = TestDatabaseCreator.createUnlimitedConcurrencyTestDatabase();

        assertTrue(ConcurrencyMonitor.instance().isUnlimited());
        assertFalse(ConcurrencyMonitor.instance().isLimited());
    }

}
