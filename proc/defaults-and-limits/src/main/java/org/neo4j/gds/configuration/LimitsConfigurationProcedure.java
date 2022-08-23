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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Here in the procedure we parse parameters and handle authentication, because that is closely tied to Neo4j
 */
public class LimitsConfigurationProcedure {
    @SuppressWarnings("WeakerAccess")
    @Context
    public KernelTransaction kernelTransaction;

    @SuppressWarnings("WeakerAccess")
    @Context
    public Username username;

    /**
     * If username is supplied, we find the _effective defaults_ for that user. So the meld of global and personal
     * defaults, without specifying where each came from. Personal defaults take precedence.
     */
    @Procedure("gds.alpha.config.limits.list")
    @Description("List limits; global by default, but also optionally for a specific user and/ or key")
    public Stream<LimitSetting> listLimits(@Name(value = "parameters", defaultValue = "{}") Map<String, Object> parameters) {
        return getFacade().listLimits(
            getOperatorName(),
            isAdministrator(),
            getAsOptionalString(parameters, "username"),
            getAsOptionalString(parameters, "key")
        );
    }

    /**
     * If username is supplied we set the default just for that user; globally otherwise.
     *
     * @param username notice this trick for optional parameters in procedure invocations.
     */
    @Procedure("gds.alpha.config.limits.set")
    @Description("Set a limit; global by, default, but also optionally for a specific user")
    public void setLimit(
        @Name(value = "key") String key,
        @Name(value = "value") Object value,
        @Name(value = "username", defaultValue = DefaultsAndLimitsConstants.DummyUsername) String username
    ) {
        getFacade().setLimit(
            getOperatorName(),
            isAdministrator(),
            getUsername(username),
            key,
            value
        );
    }

    /**
     * Right. Neo4j's procedure framework _actively prevents_ having fields that are not @Context annotated, so what
     * does one do when one wants to inject behaviour during tests? No field injection in constructor - subclass this
     * when you want to inject test dependencies.
     */
    LimitsConfigurationFacade getFacade() {
        return new LimitsConfigurationFacade(LimitsConfiguration.Instance);
    }

    /**
     * See {@link org.neo4j.gds.configuration.LimitsConfigurationProcedure#getFacade()}
     */
    String getOperatorName() {
        return this.username.username();
    }

    /**
     * See {@link org.neo4j.gds.configuration.LimitsConfigurationProcedure#getFacade()}
     */
    boolean isAdministrator() {
        return DefaultsAndLimitsPredicates.IsAdministrator(kernelTransaction);
    }

    /**
     * We use this to turn missing parameters into a firmer contract using {@link java.util.Optional}
     */
    private Optional<String> getAsOptionalString(Map<String, Object> parameters, String key) {
        if (!parameters.containsKey(key)) return Optional.empty();

        Object valueAsObject = parameters.get(key);

        String valueAsString = assertValueIsString(key, valueAsObject);

        return Optional.of(valueAsString);
    }

    private String assertValueIsString(String key, Object valueAsObject) {
        if (valueAsObject instanceof String) return (String) valueAsObject;

        throw new IllegalArgumentException(
            formatWithLocale(
                "Supplied parameter '%s' has the wrong type (%s), must be a string",
                key,
                valueAsObject
            )
        );
    }

    /**
     * Here is the trick for letting users do procedure calls with optional parameters. We match a dummy in that case.
     */
    private Optional<String> getUsername(String username) {
        if (username.equals(DefaultsAndLimitsConstants.DummyUsername)) return Optional.empty();

        return Optional.of(username);
    }
}
