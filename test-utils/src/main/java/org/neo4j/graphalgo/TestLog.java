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

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class TestLog extends AbstractLog {

    private final List<String> logMessages = new ArrayList<>();

    private final Logger logger = new Logger() {

        @Override
        public void log(String message) {
            logMessages.add(message);
        }

        @Override
        public void log(String message, Throwable throwable) {
            log(message);
        }

        @Override
        public void log(String format, Object... arguments) {
            log(String.format(format, arguments));
        }

        @Override
        public void bulk(Consumer<Logger> consumer) {
            throw new UnsupportedOperationException();
        }
    };

    public List<String> getLogMessages() {
        return logMessages;
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public Logger debugLogger() {
        return logger;
    }

    @Override
    public Logger infoLogger() {
        return logger;
    }

    @Override
    public Logger warnLogger() {
        return logger;
    }

    @Override
    public Logger errorLogger() {
        return logger;
    }

    @Override
    public void bulk(Consumer<Log> consumer) {
        consumer.accept(this);
    }
}
