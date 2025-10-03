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

import org.neo4j.gds.core.utils.progress.JobId;
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
    private final JobId jobId;

    private RequestScopedLog(Log log, JobId jobId) {
        this.log = log;
        this.jobId = jobId;
    }

    static RequestScopedLog create(Log log, JobId jobId) {
        log(log, "[%s] Algorithm processing commencing", jobId);

        return new RequestScopedLog(log, jobId);
    }

    void onLoadingGraph() {
        log(log, "[%s] Loading graph", jobId);
    }

    void onComputing() {
        log(log, "[%s] Computing algorithm", jobId);
    }

    void onProcessingResult() {
        log(log, "[%s] Processing algorithm result", jobId);
    }

    void onRenderingOutput() {
        log(log, "[%s] Rendering output", jobId);
    }

    /**
     * We get a hook where we can tell when stuff finished
     */
    @Override
    public void close() {
        log(log, "[%s] Algorithm processing complete", jobId);
    }

    private static void log(Log log, String template, JobId jobId) {
        @SuppressWarnings("PatternValidation") var logMessage = StringFormatting.formatWithLocale(template, jobId.asString());

        log.info(logMessage);
    }
}
