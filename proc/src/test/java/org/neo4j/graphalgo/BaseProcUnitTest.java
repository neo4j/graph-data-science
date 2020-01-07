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

package org.neo4j.graphalgo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseProcUnitTest {

    private TestLog log;

    private BaseProc baseProc;

    @BeforeEach
    void setup() {
        log = new TestLog();
        baseProc = new BaseProc() {};
        baseProc.log = log;
    }

    @Test
    void testRunWithExceptionLoggingWithRunnable() {
        RuntimeException exception = new RuntimeException("Exception when using Runnable");
        Runnable runnable = () -> {
            throw exception;
        };

        assertThrows(RuntimeException.class, () -> baseProc.runWithExceptionLogging("test message", runnable));
        assertTrue(log.containsMessage("warn", "test message - Exception when using Runnable"));
    }

    @Test
    void testRunWithExceptionLoggingWithSupplier() {
        RuntimeException exception = new RuntimeException("Exception when using Supplier");
        Supplier<?> supplier = () -> {
            throw exception;
        };

        assertThrows(RuntimeException.class, () -> baseProc.runWithExceptionLogging("test message", supplier));
        assertTrue(log.containsMessage("warn", "test message - Exception when using Supplier"));
    }

}