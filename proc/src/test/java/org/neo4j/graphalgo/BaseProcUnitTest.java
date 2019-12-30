/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.logging.Log;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class BaseProcUnitTest {

    @Mock
    private Log mockLog;

    @Spy
    private BaseProc baseProc;

    @BeforeEach
    void setup() {
        baseProc.log = mockLog;
    }

    @Test
    void testRunWithExceptionLoggingWithRunnable() {
        RuntimeException exception = new RuntimeException();
        Runnable runnable = () -> {
            throw exception;
        };

        assertThrows(RuntimeException.class, () -> baseProc.runWithExceptionLogging("test message", runnable));
        verify(mockLog, times(1)).warn("test message", exception);
        verifyNoMoreInteractions(mockLog);
    }

    @Test
    void testRunWithExceptionLoggingWithSupplier() {
        RuntimeException exception = new RuntimeException();
        Supplier<?> supplier = () -> {
            throw exception;
        };

        assertThrows(RuntimeException.class, () -> baseProc.runWithExceptionLogging("test message", supplier));
        verify(mockLog, times(1)).warn("test message", exception);
        verifyNoMoreInteractions(mockLog);
    }

}