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
package org.neo4j.graphalgo.compat;

import org.neo4j.kernel.internal.Version;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum Neo4jVersion {
    V_4_0,
    V_4_1,
    V_4_2;

    @Override
    public String toString() {
        switch (this) {
            case V_4_0:
                return "4.0";
            case V_4_1:
                return "4.1";
            case V_4_2:
                return "4.2";
            default:
                throw new IllegalArgumentException("Unexpected value: " + this + " (sad java 😞)");
        }
    }

    static final String OVERRIDE_VERSION_PROPERTY = "org.neo4j.gds.neo4j-version.override";

    public static Neo4jVersion findNeo4jVersion() {
        return parse(neo4jVersion());
    }

    static String neo4jVersion() {
        // Aura relevant implementation detail
        // First, we read the Neo4j Kernel version
        // This version is determined by checking for (in order)
        //   - unsupported.neo4j.custom.version system property
        //   - Implementation-Version property in META-INF/MANIFEST.MF
        //   - "dev"
        // For on-prem deployments, this will get us the correct version
        // Aura override the version using the System property to something else
        // where we no longer would deliver the correct version.
        // As a consequence, we will introduce our own System property,
        // where one can set the correct Version. Using this System property,
        // Aura is free to set the correct version for GDS without any workarounds.
        // If that is not defined, we will use the Neo4j Kernel Version
        // but will try to check for special Aura version string and treat that differently

        var neo4jVersion = System.getProperty(OVERRIDE_VERSION_PROPERTY);
        if (neo4jVersion == null || neo4jVersion.isBlank()) {
            neo4jVersion = Version.getNeo4jVersion();
        }
        if (neo4jVersion.contains("aura") || neo4jVersion.contains("Aura")) {
            neo4jVersion = "aura";
        }
        return neo4jVersion;
    }

    static Neo4jVersion parse(CharSequence version) {
        var majorVersion = Pattern.compile("[.-]")
            .splitAsStream(version)
            .limit(2)
            .collect(Collectors.joining("."));
        switch (majorVersion) {
            case "4.0":
                return Neo4jVersion.V_4_0;
            case "4.1":
                return Neo4jVersion.V_4_1;
            case "4.2":
            case "aura":
            case "dev":
                return Neo4jVersion.V_4_2;
            default:
                throw new UnsupportedOperationException("Cannot run on Neo4j Version " + version);
        }
    }
}
