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
package org.neo4j.gds.compat;

import org.neo4j.kernel.internal.Version;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum Neo4jVersion {
    V_4_3,
    V_4_4,
    V_4_4_drop10;

    @Override
    public String toString() {
        switch (this) {
            case V_4_3:
                return "4.3";
            case V_4_4:
                return "4.4";
            case V_4_4_drop10:
                return "4.4.7-drop01.0";
            default:
                throw new IllegalArgumentException("Unexpected value: " + this.name() + " (sad java 😞)");
        }
    }

    public static Neo4jVersion findNeo4jVersion() {
        return Neo4jVersionHolder.VERSION;
    }

    private static final class Neo4jVersionHolder {
        private static final Neo4jVersion VERSION = parse(neo4jVersion());
    }

    static String neo4jVersion() {
        var neo4jVersion = Objects.requireNonNullElse(Version.class.getPackage().getImplementationVersion(), "dev");
        // some versions have a build thing attached at the end
        // e.g. 4.0.8,8e921029f7daebacc749034f0cb174f1f2c7a258
        // This regex follows the logic from org.neo4j.kernel.internal.Version.parseReleaseVersion
        Pattern pattern = Pattern.compile(
            "(\\d+" +                  // Major version
            "\\.\\d+" +                // Minor version
            "(\\.\\d+)?" +             // Optional patch version
            "(-?[^,]+)?)" +            // Optional marker, like M01, GA, SNAPSHOT - anything other than a comma
            ".*"                       // Anything else, such as git revision
        );
        var matcher = pattern.matcher(neo4jVersion);
        if (matcher.find()) {
            return matcher.group(1);
        }
        // If no match is found, return the full version.
        return neo4jVersion;
    }

    static Neo4jVersion parse(String version) {
        if ("4.4.7-drop01.0".equals(version) || "4.4.6-drop01.0".equals(version)) {
            return Neo4jVersion.V_4_4_drop10;
        }
        var majorVersion = Pattern.compile("[.-]")
            .splitAsStream(version)
            .limit(2)
            .collect(Collectors.joining("."));
        switch (majorVersion) {
            case "4.3":
                return Neo4jVersion.V_4_3;
            case "4.4":
                return Neo4jVersion.V_4_4;
            default:
                throw new UnsupportedOperationException("Cannot run on Neo4j Version " + version);
        }
    }
}
