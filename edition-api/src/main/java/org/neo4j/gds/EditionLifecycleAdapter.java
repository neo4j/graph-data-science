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
package org.neo4j.gds;

import org.neo4j.configuration.Config;
import org.neo4j.gds.concurrency.ConcurrencyValidatorBuilder;
import org.neo4j.gds.concurrency.ConcurrencyValidatorService;
import org.neo4j.gds.concurrency.PoolSizesProvider;
import org.neo4j.gds.concurrency.PoolSizesService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.util.Comparator;
import java.util.ServiceLoader;
import java.util.function.Function;

public class EditionLifecycleAdapter extends LifecycleAdapter {

    private final Config config;
    private final GlobalProcedures globalProceduresRegistry;

    EditionLifecycleAdapter(Config config, GlobalProcedures globalProceduresRegistry) {
        this.config = config;
        this.globalProceduresRegistry = globalProceduresRegistry;
    }

    @Override
    public void init() {
        var licenseState = registerLicenseState();
        setupConcurrencyValidator(licenseState);
        setupPoolSizes(licenseState);
    }

    private LicenseState registerLicenseState() {
        var licensingServiceBuilder = loadServiceByPriority(
            LicensingServiceBuilder.class,
            LicensingServiceBuilder::priority
        );
        var licensingService = licensingServiceBuilder.build(config);

        globalProceduresRegistry.registerComponent(LicenseState.class, (context) -> licensingService.get(), true);
        return licensingService.get();
    }

    private void setupConcurrencyValidator(LicenseState licenseState) {
        var concurrencyValidatorBuilder = loadServiceByPriority(
            ConcurrencyValidatorBuilder.class,
            ConcurrencyValidatorBuilder::priority
        );

        ConcurrencyValidatorService.validator(concurrencyValidatorBuilder.build(licenseState));
    }

    private void setupPoolSizes(LicenseState licenseState) {
        var poolSizesProvider = loadServiceByPriority(
            PoolSizesProvider.class,
            PoolSizesProvider::priority
        );

        PoolSizesService.poolSizes(poolSizesProvider.get(licenseState));
    }

    private <T> T loadServiceByPriority(Class<T> serviceClass, Function<T, Integer> comparingFunction) {
        return ServiceLoader.load(serviceClass)
            .stream()
            .map(ServiceLoader.Provider::get)
            .max(Comparator.comparing(comparingFunction))
            .orElseThrow(() -> new LinkageError("Could not load " + serviceClass + " implementation"));
    }
}
