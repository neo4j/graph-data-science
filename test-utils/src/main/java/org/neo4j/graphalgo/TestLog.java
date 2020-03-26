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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public class TestLog extends AbstractLog {
    public static String DEBUG = "debug";
    public static String INFO = "info";
    public static String WARN = "warn";
    public static String ERROR = "error";

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
            log(String.format(format, arguments));
        }

        @Override
        public void bulk(Consumer<Logger> consumer) {
            consumer.accept(this);
        }

        private void logMessage(String message) {
            ConcurrentLinkedQueue<String> messageList = messages.computeIfAbsent(level, (ignore) -> new ConcurrentLinkedQueue<>());
            messageList.add(message);
        }
    }
}


