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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.function.Supplier;

public interface ProgressLogger {

    ProgressLogger NULL_LOGGER = NullProgressLogger.INSTANCE;

    Supplier<String> NO_MESSAGE = () -> null;

    default void logProgress() {
        logProgress(NO_MESSAGE);
    };

    void logProgress(Supplier<String> msgFactory);

    default void logProgress(long progress) {
        logProgress(progress, NO_MESSAGE);
    };

    void logProgress(long progress, Supplier<String> msgFactory);

    void logMessage(Supplier<String> msg);

    default void logMessage(String msg) {
        logMessage(() -> msg);
    }

    void reset(long newTaskVolume);

    Log getLog();

    @Deprecated
    void logProgress(double percentDone, Supplier<String> msg);

    @Deprecated
    default void logProgress(double numerator, double denominator, Supplier<String> msg) {
        logProgress(numerator / denominator, msg);
    }

    @Deprecated
    default void logProgress(double numerator, double denominator) {
        logProgress(numerator, denominator, NO_MESSAGE);
    }

    @Deprecated
    default void logProgress(double percentDone) {
        logProgress(percentDone, NO_MESSAGE);
    }


    enum NullProgressLogger implements ProgressLogger {
        INSTANCE;

        @Override
        public void logProgress(Supplier<String> msgFactory) {

        }

        @Override
        public void logProgress(long progress, Supplier<String> msgFactory) {

        }

        @Override
        public void logMessage(Supplier<String> msg) {

        }

        @Override
        public void reset(long newTaskVolume) {

        }

        @Override
        public Log getLog() {
            return NullLog.getInstance();
        }

        @Override
        public void logProgress(double percentDone, Supplier<String> msg) {

        }
    };
}
