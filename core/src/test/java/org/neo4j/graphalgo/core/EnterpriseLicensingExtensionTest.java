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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.Test;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnterpriseLicensingExtensionTest {

    @Test
    void shouldSetMonitorFalse() {
        new TestDatabaseManagementServiceBuilder()
            .addExtension(new EnterpriseLicensingExtension())
            .impermanent()
            .build()
            .database(Settings.defaultDatabaseName());

        assertFalse(GdsEdition.instance().isOnEnterpriseEdition());
        assertTrue(GdsEdition.instance().isOnCommunityEdition());
    }

    @Test
    void shouldSetMonitorTrue() {
        new TestDatabaseManagementServiceBuilder()
            .addExtension(new EnterpriseLicensingExtension())
            .setConfig(Settings.enterpriseLicenseKey(), "TXlTdXBlclNlY3JldE1lc3NhZ2U9PT09PT1TSUdOQVRVUkU9PT09PT1hQ1hkbHByZmdMYVNEanZzS2ZVQWFrMXpvVEMrTit3aGRBeUxDdWZuRFRXVlNUTExURlpJREtWWmorZXB1ajllaXVVd0hVcDZqbGxDTGEwY2lHL3VkUDhMWTR0UjY3aDhwcWo2bk5pUzBnRktyL1hCTkpVQ3RIL3FJbDY2aWllQXJncEw2T1NxSGN3Rm85Zzc2bDVaVkxqYktucmNhSFNLVkQ5TVAwdDkrbEx0d0hpcFVRRTVHWGM5dkhqMTczTzhwSytIU0hLMzNqSFEvSHdrUWJJUHlhTC9VM0Z6TFlqbXFnYm8zZDJyVU9IQndicjRFVkZyczRPMVhCRGRtTXBmNE1HejEyc050djJHZlVkeEo3SkFvSXpwN1EyQmx2TzFrQzR3b01FSHBKbjZnN2JBbFE1UUs1WVpJbU1TdU4xZzdPKzk2YkROTk5iVmsxMjNnT1RZRXc9PQ==")
            .impermanent()
            .build()
            .database(Settings.defaultDatabaseName());

        assertTrue(GdsEdition.instance().isOnEnterpriseEdition());
        assertFalse(GdsEdition.instance().isOnCommunityEdition());
    }
}
