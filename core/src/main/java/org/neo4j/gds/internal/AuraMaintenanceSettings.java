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
package org.neo4j.gds.internal;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import java.nio.file.Path;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;

@ServiceProvider
public final class AuraMaintenanceSettings implements SettingsDeclaration {

    @Description("Enable maintenance function.")
    @DocumentedDefaultValue("false")
    public static final Setting<Boolean> maintenance_function_enabled = newBuilder(
        "gds.maintenance_function_enabled",
        BOOL,
        false
    ).build();

    @Description("Use maximum memory estimation in procedure memory guard.")
    @DocumentedDefaultValue("false")
    public static final Setting<Boolean> validate_using_max_memory_estimation = newBuilder(
        "gds.validate_using_max_memory_estimation",
        BOOL,
        false
    ).build();

    @Description("Sets the backup location for file based exports.")
    public static final Setting<Path> backup_location_setting = newBuilder(
        "gds.backup.location",
        PATH,
        null
    ).build();

    @Description("Sets the maximum number of allowed backups in the backup location.")
    public static final Setting<Integer> max_number_of_backups = newBuilder(
        "gds.backup.max_number_of_backups",
        INT,
        -1
    ).build();
}
