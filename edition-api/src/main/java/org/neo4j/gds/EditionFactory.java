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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Optional;

@ServiceProvider
public class EditionFactory extends ExtensionFactory<EditionFactory.Dependencies> {

    private final Optional<LicenseState> licenseState;

    public EditionFactory() {
        super(ExtensionType.GLOBAL, "gds.edition_factory");
        this.licenseState = Optional.empty();
    }

    @TestOnly
    public EditionFactory(LicenseState licenseState) {
        super(ExtensionType.GLOBAL, "gds.edition_factory");
        this.licenseState = Optional.of(licenseState);
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new EditionLifecycleAdapter(
            context,
            dependencies.config(),
            dependencies.globalProceduresRegistry(),
            licenseState
        );
    }

    interface Dependencies {
        Config config();

        GlobalProcedures globalProceduresRegistry();
    }
}
