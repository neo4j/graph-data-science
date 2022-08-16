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
package org.neo4j.gds.configuration;

import org.neo4j.gds.core.Username;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * Here in the procedure we parse parameters and handle authentication, because that is closely tied to Neo4j
 */
public class DefaultsAndLimitsConfigurationProcedure {
    @SuppressWarnings("WeakerAccess")
    @Context
    public KernelTransaction kernelTransaction;

    @SuppressWarnings("WeakerAccess")
    @Context
    public Username username;

    private final BooleanSupplier isAdministratorPredicate;
    private final DefaultsAndLimitsConfigurationFacade facade;

    /**
     * Exists to satisfy procedure framework.
     */
    @SuppressWarnings("unused")
    public DefaultsAndLimitsConfigurationProcedure() {
        isAdministratorPredicate = this::isAdministrator;
        facade = new DefaultsAndLimitsConfigurationFacade(DefaultsConfiguration.Instance);
    }

    DefaultsAndLimitsConfigurationProcedure(
        KernelTransaction kernelTransaction,
        Username username,
        BooleanSupplier isAdministratorPredicate,
        DefaultsAndLimitsConfigurationFacade facade
    ) {
        this.kernelTransaction = kernelTransaction;
        this.username = username;
        this.isAdministratorPredicate = isAdministratorPredicate;
        this.facade = facade;
    }

    /**
     * If username is supplied, we find the _effective defaults_ for that user. So the meld of global and personal
     * defaults, without specifying where each came from.
     *
     * Or we add a marker, type = global or something? Not until someone asks for it!
     */
    @Procedure("gds.config.defaults.list")
    @Description("List defaults; global by default, but also optionally for a specific user and/ or key")
    public Stream<DefaultSetting> listDefaults(
        @Name(value = "username") String username,
        @Name(value = "key") String key
    ) {
        return facade.listDefaults(
            this.username.username(),
            isAdministratorPredicate.getAsBoolean(),
            Optional.ofNullable(username),
            Optional.ofNullable(key)
        );
    }

    // stolen from BaseProc
    private boolean isAdministrator() {
        if (kernelTransaction == null) {
            // No transaction available (likely we're in a test), no-one is admin here
            return false;
        }
        // this should be the same as the predefined role from enterprise-security
        // com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN
        String PREDEFINED_ADMIN_ROLE = "admin";
        return kernelTransaction.securityContext().roles().contains(PREDEFINED_ADMIN_ROLE);
    }
}
