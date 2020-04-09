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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

@ServiceProvider
public final class GcListenerExtension extends ExtensionFactory<GcListenerExtension.Dependencies> {
    // Initialize with max available memory. Everything that is used at this point in time
    //  _could_ be garbage and we want to err on the side of seeing more free heap.
    // It also has the effect that we allow all operations that theoretically fit into memory
    //  if the extension does never load.
    private static final AtomicLong freeMemoryAfterLastGc = new AtomicLong(Runtime.getRuntime().maxMemory());

    public GcListenerExtension() {
        super("gds.heap-control.gc-listener");
    }

    public static long freeMemory() {
        return freeMemoryAfterLastGc.get();
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new GcListenerInstaller(
            dependencies.logService().getInternalLogProvider(),
            ManagementFactory.getGarbageCollectorMXBeans(),
            freeMemoryAfterLastGc
        );
    }

    interface Dependencies {
        LogService logService();
    }
}
