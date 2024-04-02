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
package org.neo4j.gds.core.utils;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class ClockService {

    private static final Clock SYSTEM_CLOCK = Clock.systemUTC();

    private static final AtomicReference<Clock> CLOCK = new AtomicReference<>(SYSTEM_CLOCK);

    public static void setClock(Clock clock) {
        CLOCK.set(clock);
    }

    public static Clock clock() {
        return CLOCK.get();
    }

    private ClockService() {}

    public static <T extends Clock> void runWithClock(T clock, Consumer<T> runnable) {
        Clock previousClock = CLOCK.getAndSet(clock);
        try {
            runnable.accept(clock);
        } finally {
            CLOCK.set(previousClock);
        }
    }
}
