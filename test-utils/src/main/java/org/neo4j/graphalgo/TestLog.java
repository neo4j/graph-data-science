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
package org.neo4j.graphalgo;

import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TestLog extends AbstractLog {
    public static final String DEBUG = "debug";
    public static final String INFO = "info";
    public static final String WARN = "warn";
    public static final String ERROR = "error";

    private final ConcurrentMap<String, ConcurrentLinkedQueue<String>> messages;

    public TestLog() {
        messages = new ConcurrentHashMap<>(3);
    }

    public boolean containsMessage(String level, String fragment) {
        ConcurrentLinkedQueue<String> messageList = messages.getOrDefault(level, new ConcurrentLinkedQueue<>());
        return messageList.stream().anyMatch((message) -> message.contains(fragment));
    }

    public boolean hasMessages(String level) {
        return !messages.getOrDefault(level, new ConcurrentLinkedQueue<>()).isEmpty();
    }

    public ArrayList<String> getMessages(String level) {
        return new ArrayList<>(messages.getOrDefault(level, new ConcurrentLinkedQueue<>()));
    }

    @SuppressForbidden(reason = "test log can print")
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
    public void debug(String message) {
        debugLogger().log(message);
    }

    @Override
    public void debug(String message, Throwable throwable) {
        debugLogger().log(message, throwable);
    }

    @Override
    public void debug(String format, Object... arguments) {
        debugLogger().log(format, arguments);
    }

    @Override
    public void info(String message) {
        infoLogger().log(message);
    }

    @Override
    public void info(String message, Throwable throwable) {
        infoLogger().log(message, throwable);
    }

    @Override
    public void info(String format, Object... arguments) {
        infoLogger().log(format, arguments);
    }

    @Override
    public void warn(String message) {
        warnLogger().log(message);
    }

    @Override
    public void warn(String message, Throwable throwable) {
        warnLogger().log(message, throwable);
    }

    @Override
    public void warn(String format, Object... arguments) {
        warnLogger().log(format, arguments);
    }

    @Override
    public void error(String message) {
        errorLogger().log(message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        errorLogger().log(message, throwable);
    }

    @Override
    public void error(String format, Object... arguments) {
        errorLogger().log(format, arguments);
    }

    @Override
    public void bulk(Consumer<Log> consumer) {
        consumer.accept(this);
    }

    class TestLogger implements Logger {
        private final String level;
        private final ConcurrentMap<String, ConcurrentLinkedQueue<String>> messages;


        TestLogger(String level, ConcurrentMap<String, ConcurrentLinkedQueue<String>> messages) {
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
            log(formatWithLocale(format, arguments));
        }

        @Override
        public void bulk(Consumer<Logger> consumer) {
            consumer.accept(this);
        }

        private void logMessage(String message) {
            ConcurrentLinkedQueue<String> messageList = messages.computeIfAbsent(
                level,
                (ignore) -> new ConcurrentLinkedQueue<>()
            );
            messageList.add(message);
        }
    }
}


