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
package org.neo4j.graphalgo.core.concurrency;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.graphalgo.config.ConcurrencyValidation.CORE_LIMITATION_SETTING;

@Service.Implementation(KernelExtensionFactory.class)
public final class ConcurrencyControllerExtension extends KernelExtensionFactory<ConcurrencyControllerExtension.Dependencies> {

    public ConcurrencyControllerExtension() {
        super(ExtensionType.DATABASE, "gds.concurrency-limitation");
    }

    @Override
    public Lifecycle newInstance(
        KernelContext context, Dependencies dependencies
    ) {
        return new LifecycleAdapter()
        {
            @Override
            public void init()
            {
                boolean unlimitedCores = dependencies
                    .config()
                    .get(CORE_LIMITATION_SETTING);
                ConcurrencyMonitor concurrencyMonitor = ConcurrencyMonitor.instance();
                if (unlimitedCores) {
                    concurrencyMonitor.setUnlimited();
                } else {
                    concurrencyMonitor.setLimited();
                }
            }

            @Override
            public void shutdown()
            {
            }
        };
    }

    interface Dependencies {
        Config config();
    }
}
