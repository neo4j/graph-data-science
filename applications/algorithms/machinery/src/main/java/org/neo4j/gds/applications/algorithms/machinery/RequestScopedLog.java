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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.utils.StringFormatting;

/**
 * Logging enhanced with correlation id.
 * So that we may know when an algorithm began, ended, and what it did in between.
 * Use this in a try-with-resources for that nice finalizer.
 */
@SuppressWarnings("ClassCanBeRecord")
final class RequestScopedLog implements AutoCloseable {
    private final Log log;
    private final RequestCorrelationId requestCorrelationId;

    private RequestScopedLog(Log log, RequestCorrelationId requestCorrelationId) {
        this.log = log;
        this.requestCorrelationId = requestCorrelationId;
    }

    static RequestScopedLog create(Log log, RequestCorrelationId requestCorrelationId) {
        log(log, "[%s] Algorithm processing commencing", requestCorrelationId);

        return new RequestScopedLog(log, requestCorrelationId);
    }

    void onLoadingGraph() {
        log(log, "[%s] Loading graph", requestCorrelationId);
    }

    void onComputing() {
        log(log, "[%s] Computing algorithm", requestCorrelationId);
    }

    void onProcessingResult() {
        log(log, "[%s] Processing algorithm result", requestCorrelationId);
    }

    void onRenderingOutput() {
        log(log, "[%s] Rendering output", requestCorrelationId);
    }

    /**
     * We get a hook where we can tell when stuff finished
     */
    @Override
    public void close() {
        log(log, "[%s] Algorithm processing complete", requestCorrelationId);
    }

    private static void log(Log log, String template, RequestCorrelationId requestCorrelationId) {
        @SuppressWarnings("PatternValidation") var logMessage = StringFormatting.formatWithLocale(
            template,
            requestCorrelationId
        );

        log.info(logMessage);
    }
}
