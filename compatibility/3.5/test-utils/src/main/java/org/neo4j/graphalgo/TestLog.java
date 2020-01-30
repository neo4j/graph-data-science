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

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TestLog extends AbstractLog {
    public static String DEBUG = "debug";
    public static String INFO = "info";
    public static String WARN = "warn";
    public static String ERROR = "error";

    private final Map<String, List<String>> messages;

    public TestLog() {
        messages = new HashMap<>(3);
    }

    public boolean containsMessage(String level, String fragment) {
        List<String> messageList = messages.getOrDefault(level, Collections.emptyList());
        return messageList.stream().anyMatch((message) -> message.contains(fragment));
    }

    public boolean hasMessages(String level) {
        return !messages.getOrDefault(level, Collections.emptyList()).isEmpty();
    }

    public void printMessages() {
        System.out.println("TestLog Messages: " + messages);
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public Logger debugLogger() {
        return new TestLogger(DEBUG, messages);
    }

    @Override
    public Logger infoLogger() {
        return new TestLogger(INFO, messages);
    }

    @Override
    public Logger warnLogger() {
        return new TestLogger(WARN, messages);
    }

    @Override
    public Logger errorLogger() {
        return new TestLogger(ERROR, messages);
    }

    @Override
    public void bulk(Consumer<Log> consumer) {
        consumer.accept(this);
    }

    class TestLogger implements Logger {
        private final String level;
        private final Map<String, List<String>> messages;


        TestLogger(String level, Map<String, List<String>> messages) {
            this.level = level;
            this.messages = messages;
        }

        @Override
        public void log(String message) {
            logMessage(message);
        }

        @Override
        public void log(String message, Throwable throwable) {
            logMessage(message + " - " + throwable.getMessage());
        }

        @Override
        public void log(String format, Object... arguments) {
            log(String.format(format, arguments));
        }

        @Override
        public void bulk(Consumer<Logger> consumer) {
            consumer.accept(this);
        }

        private void logMessage(String message) {
            List<String> messageList = messages.computeIfAbsent(level, (ignore) -> new ArrayList<>());
            messageList.add(message);
        }
    }
}


