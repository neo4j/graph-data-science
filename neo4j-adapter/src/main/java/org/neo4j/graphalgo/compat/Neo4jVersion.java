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

import java.util.Objects;
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

    public static Neo4jVersion findNeo4jVersion() {
        return parse(neo4jVersion());
    }

    static String neo4jVersion() {
        // Aura relevant implementation detail
        //
        // We don't call `Version.getNeo4jVersion()` directly, as this one
        // allows for a system property override. This override is used
        // by Aura for reasons relevant to them.
        // The version set by Aura does not necessarily reflect the actual Neo4j version,
        // that is, they might set the version to `4.0-Aura` while Neo4j is actually at version 4.2.x.
        //
        // For this reason, we read the _actual_ Neo4j version without checking the override property.
        var neo4jVersion = Objects.requireNonNullElse(Version.class.getPackage().getImplementationVersion(), "dev");
        // some versions have a build thing attached at the end
        // e.g. 4.0.8,8e921029f7daebacc749034f0cb174f1f2c7a258
        // This regex follows the logic from org.neo4j.kernel.internal.Version.parseReleaseVersion
        var pattern = Pattern.compile("^(\\d+\\.\\d+(:?\\.\\d+)?(:?-?[^,]+)?)");
        var matcher = pattern.matcher(neo4jVersion);
        if (matcher.find()) {
            return matcher.group(1);
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
