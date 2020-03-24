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

import org.neo4j.graphalgo.core.utils.ProgressLogger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

public class TestProgressLogger implements ProgressLogger {

    public static final TestProgressLogger INSTANCE = new TestProgressLogger();

    private final BlockingQueue<String> messages;
    private final BlockingQueue<Double> percentages;

    public TestProgressLogger() {
        this.messages = new ArrayBlockingQueue<>(100);
        this.percentages = new ArrayBlockingQueue<Double>(100);
    }


    @Override
    public void logProgress(double percentDone, Supplier<String> msg) {
        messages.add(msg.get() == null ? "NULL" : msg.get());
        percentages.add(percentDone);
    }

    @Override
    public void log(Supplier<String> msg) {
        messages.add(msg.get() == null ? "NULL" : msg.get());
    }

    public BlockingQueue<String> getMessages() {
        return messages;
    }

    public BlockingQueue<Double> getPercentages() {
        return percentages;
    }
}
