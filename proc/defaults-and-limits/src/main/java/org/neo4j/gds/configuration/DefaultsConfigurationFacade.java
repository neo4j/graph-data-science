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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Here in the UI layer we need to take care of authorisation and dispatch.
 */
class DefaultsConfigurationFacade {
    private final DefaultsConfiguration defaultsConfiguration;

    DefaultsConfigurationFacade(DefaultsConfiguration defaultsConfiguration) {
        this.defaultsConfiguration = defaultsConfiguration;
    }

    /**
     * Returns all global default settings sorted by key.
     *
     * If you specify a username, default settings for that user are overlaid. You need to be an administrator in order
     * to list default settings for other users.
     *
     * If you specify a key, only the corresponding default setting for that key is returned.
     */
    Stream<DefaultSetting> listDefaults(
        String usernameOfOperator,
        boolean operatorIsAdministrator,
        Optional<String> usernameOfTarget,
        Optional<String> key
    ) {
        assertAuthorisedToList(usernameOfOperator, operatorIsAdministrator, usernameOfTarget);

        Map<String, Object> defaults = defaultsConfiguration.list(usernameOfTarget, key);

        return defaults.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(e -> new DefaultSetting(e.getKey(), e.getValue()));
    }

    /**
     * You are authorised to see settings for other users iff you are an administrator
     */
    void assertAuthorisedToList(String usernameOfOperator, boolean operatorIsAdministrator, Optional<String> username) {
        if (username.isEmpty()) return; // there is nothing to authorise

        if (operatorIsAdministrator) return; // administrator is allowed anything

        if (usernameOfOperator.equals(username.get())) return;

        throw new IllegalArgumentException(formatWithLocale(
            "User '%s' not authorized to list default settings for user '%s'",
            usernameOfOperator,
            username.get()
        ));
    }

    void setDefault(
        String usernameOfOperator,
        boolean operatorIsAdministrator,
        Optional<String> username,
        String key,
        Object value
    ) {
        assertAuthorisedToSet(usernameOfOperator, operatorIsAdministrator, username);

        defaultsConfiguration.set(key, value, username);
    }

    private void assertAuthorisedToSet(
        String usernameOfOperator,
        boolean operatorIsAdministrator,
        Optional<String> username
    ) {
        if (operatorIsAdministrator) return; // administrators can do anything

        if (username.isEmpty())
            throw new IllegalArgumentException(formatWithLocale(
                "User '%s' not authorized to set global defaults",
                usernameOfOperator
            ));

        if (usernameOfOperator.equals(username.get())) return; // you can set your own defaults

        throw new IllegalArgumentException(formatWithLocale(
            "User '%s' not authorized to set default for user '%s'",
            usernameOfOperator,
            username.get()
        ));
    }
}
