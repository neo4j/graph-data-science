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
package org.neo4j.graphalgo.core.utils.mem;

import org.neo4j.graphalgo.annotation.IdenticalCompat;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryManagerMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@IdenticalCompat
final class GcListenerInstaller extends LifecycleAdapter {
    private final LogProvider logProvider;
    private final List<GarbageCollectorMXBean> gcBeans;
    private final AtomicLong freeMemory;
    private final Map<NotificationBroadcaster, NotificationListener> registeredListeners;

    GcListenerInstaller(
        LogProvider logProvider,
        Collection<GarbageCollectorMXBean> gcBeans,
        AtomicLong freeMemory
    ) {
        this.logProvider = logProvider;
        // make defensive copy
        this.gcBeans = new ArrayList<>(gcBeans);
        this.freeMemory = freeMemory;
        this.registeredListeners = new IdentityHashMap<>();
    }

    @Override
    public void init() {
        for (GarbageCollectorMXBean gcBean : this.gcBeans) {
            installGcListener(gcBean);
        }
    }

    @Override
    public void shutdown() {
        this.registeredListeners.forEach(this::uninstallGcListener);
    }

    private void installGcListener(MemoryManagerMXBean gcBean) {
        if (gcBean instanceof NotificationBroadcaster) {
            NotificationBroadcaster broadcaster = (NotificationBroadcaster) gcBean;
            Optional<NotificationListener> listener = HotSpotGcListener.install(
                this.logProvider,
                this.freeMemory,
                gcBean.getMemoryPoolNames(),
                broadcaster
            );
            listener.ifPresent(notificationListener -> this.registeredListeners.put(
                broadcaster,
                notificationListener
            ));
        }
    }

    private void uninstallGcListener(NotificationBroadcaster gcBean, NotificationListener listener) {
        try {
            gcBean.removeNotificationListener(listener);
        } catch (ListenerNotFoundException e) {
            // ignore
        }
    }
}
