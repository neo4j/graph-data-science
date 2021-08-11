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

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.compat.JobPromise;
import org.neo4j.gds.compat.JobRunner;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.RenamesCurrentThread;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class ProgressEventHandlerImpl extends LifecycleAdapter implements Runnable, ProgressEventHandler {

    private final Monitor monitor;
    private final JobRunner jobRunner;
    private final Queue<ProgressEvent> queue;

    private volatile @Nullable JobPromise job;
    private final List<Consumer<ProgressEvent>> eventListeners;

    ProgressEventHandlerImpl(
        Monitor monitor,
        JobScheduler jobScheduler,
        Queue<ProgressEvent> queue
    ) {
        this(monitor, Neo4jProxy.runnerFromScheduler(jobScheduler, Group.DATA_COLLECTOR), queue);
    }

    @TestOnly
    ProgressEventHandlerImpl(
        JobRunner jobRunner,
        Queue<ProgressEvent> queue
    ) {
        this(Monitor.EMPTY, jobRunner, queue);
    }

    private ProgressEventHandlerImpl(
        Monitor monitor,
        JobRunner jobRunner,
        Queue<ProgressEvent> queue
    ) {
        this.monitor = monitor;
        this.jobRunner = jobRunner;
        this.queue = queue;
        this.eventListeners = new ArrayList<>();
    }

    @Override
    public void registerProgressEventListener(Consumer<ProgressEvent> eventConsumer) {
        this.eventListeners.add(eventConsumer);
    }

    @Override
    public void run() {
        try (var ignored = RenamesCurrentThread.renameThread("progress-event-consumer")) {
            ProgressEvent event;
            while ((event = queue.poll()) != null && !Thread.interrupted()) {
                process(event);
            }
        }
    }

    private void process(ProgressEvent event) {
        eventListeners.forEach(progressEventConsumer -> progressEventConsumer.accept(event));
    }

    @Override
    public void start() {
        monitor.started();
        if (job != null) {
            throw new IllegalArgumentException("Already running");
        }
        job = jobRunner.scheduleAtInterval(this, 0, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        monitor.stopped();
        // pull into field to avoid racing against another stop.
        // We might cancel multiple times, but we're not running into a sneaky NPE
        var job = this.job;
        if (job == null) {
            throw new IllegalArgumentException("Not running");
        }
        job.cancel();
        this.job = null;
    }

    @TestOnly
    boolean isRunning() {
        return job != null;
    }

    interface Monitor {
        void started();

        void stopped();

        class Adapter implements Monitor {
            @Override
            public void started() {
            }

            @Override
            public void stopped() {
            }
        }

        Monitor EMPTY = new Monitor.Adapter();
    }
}
