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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author mknblch
 */
public interface ProgressLogger {

    ProgressLogger NULL_LOGGER = new ProgressLoggerAdapter(NullLog.getInstance(), "NULL");
    Supplier<String> NO_MESSAGE = () -> null;

    static ProgressLogger wrap(Log log, String task) {
        return new ProgressLoggerAdapter(log, task);
    }

    static ProgressLogger wrap(Log log, String task, long time, TimeUnit unit) {
        if (log == null || log == NullLog.getInstance() || task == null) {
            return ProgressLogger.NULL_LOGGER;
        }
        ProgressLoggerAdapter logger = new ProgressLoggerAdapter(log, task);
        if (time > 0L) {
            logger.withLogIntervalMillis((int) Math.min(unit.toMillis(time), (long) Integer.MAX_VALUE));
        }
        return logger;
    }

    void logProgress(double percentDone, Supplier<String> msg);

    void log(Supplier<String> msg);

    default void log(String msg) {
        log(() -> msg);
    }

    default void logDone(Supplier<String> msg) {
        log(msg);
    }

    default void logProgress(double numerator, double denominator, Supplier<String> msg) {
        logProgress(numerator / denominator, msg);
    }

    default void logProgress(double numerator, double denominator) {
        logProgress(numerator, denominator, NO_MESSAGE);
    }

    default void logProgress(double percentDone) {
        logProgress(percentDone, NO_MESSAGE);
    }

    default void logDone() {
        logDone(NO_MESSAGE);
    }
}
