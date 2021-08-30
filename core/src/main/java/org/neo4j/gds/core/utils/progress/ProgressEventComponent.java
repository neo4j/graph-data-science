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
package org.neo4j.gds.core.utils.progress;

import org.jctools.queues.MpscLinkedQueue;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import java.util.Queue;

final class ProgressEventComponent extends LifecycleAdapter {

    private final Log log;
    private final JobScheduler jobScheduler;
    private final Monitors globalMonitors;
    private final ProgressEventHandlerImpl.Monitor monitor;
    private final LoggingProgressEventMonitor loggingMonitor;
    private final Queue<ProgressEvent> messageQueue;
    private volatile ProgressEventHandlerImpl progressEventHandler;

    ProgressEventComponent(
        Log log,
        JobScheduler jobScheduler,
        Monitors globalMonitors
    ) {
        this.log = log;
        this.jobScheduler = jobScheduler;
        this.globalMonitors = globalMonitors;
        this.monitor = globalMonitors.newMonitor(ProgressEventHandlerImpl.Monitor.class);
        this.loggingMonitor = new LoggingProgressEventMonitor(log);
        this.messageQueue = new MpscLinkedQueue<>();
    }

    @Override
    public void start() {
        globalMonitors.addMonitorListener(loggingMonitor);
        progressEventHandler = new ProgressEventHandlerImpl(monitor, jobScheduler, messageQueue);
        progressEventHandler.start();
        this.log.info("GDS Progress event tracking is enabled");
    }

    @Override
    public void stop() {
        progressEventHandler.stop();
        progressEventHandler = null;
        globalMonitors.removeMonitorListener(loggingMonitor);
    }
}
