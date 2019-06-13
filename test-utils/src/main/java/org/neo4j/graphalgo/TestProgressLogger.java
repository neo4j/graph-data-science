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

import org.neo4j.graphalgo.core.utils.ProgressLogger;

import java.util.function.Supplier;

public class TestProgressLogger implements ProgressLogger {

    public static final ProgressLogger INSTANCE = new TestProgressLogger();

    public static final long TIMEOUT = 3000;

    private long lastLog = 0;

    @Override
    public void logProgress(double percentDone, Supplier<String> msg) {
        final long now = System.currentTimeMillis();
        if (lastLog + TIMEOUT < now) {
            lastLog = now;
            System.out.printf("[%s] %.0f%% (%s)%n", Thread.currentThread().getName(), percentDone * 100, msg.get());
        }
    }

    @Override
    public void log(Supplier<String> msg) {
        System.out.println(msg.get());
    }
}
