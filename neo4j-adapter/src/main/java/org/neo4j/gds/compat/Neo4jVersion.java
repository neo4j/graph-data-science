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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.kernel.internal.Version;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public enum Neo4jVersion {
    V_4_4,
    V_5_1,
    V_5_2,
    V_5_3,
    V_5_4,
    V_5_5,
    V_5_6,
    V_5_7,
    V_5_8,
    V_5_9,
    V_5_10,
    V_5_11,
    V_5_12,
    V_5_13,
    V_5_14,
    V_5_15,
    V_Dev;

    private static final int MINOR_DEV_VERSION = 16;

    static Neo4jVersion parse(String version) {
        var versionSegments = Pattern.compile("[.-]")
            .splitAsStream(version)
            .limit(2)
            .mapToInt(v -> {
                try {
                    return Integer.parseInt(v);
                } catch (NumberFormatException notANumber) {
                    return -1;
                }
            });

        var majorMinorVersion = IntStream.concat(versionSegments, IntStream.of(-1, -1))
            .limit(2)
            .toArray();

        var majorVersion = majorMinorVersion[0];
        var minorVersion = majorMinorVersion[1];

        if (majorVersion == 4 && minorVersion == 4) {
            return Neo4jVersion.V_4_4;
        } else if (majorVersion == 5) {
            switch (minorVersion) {
                case 6:
                    return Neo4jVersion.V_5_6;
                case 7:
                    return Neo4jVersion.V_5_7;
                case 8:
                    return Neo4jVersion.V_5_8;
                case 9:
                    return Neo4jVersion.V_5_9;
                case 10:
                    return Neo4jVersion.V_5_10;
                case 11:
                    return Neo4jVersion.V_5_11;
                case 12:
                    return Neo4jVersion.V_5_12;
                case 13:
                    return Neo4jVersion.V_5_13;
                case 14:
                    return Neo4jVersion.V_5_14;
                case 15:
                    return Neo4jVersion.V_5_15;
                default:
                    if (minorVersion >= MINOR_DEV_VERSION) {
                        return Neo4jVersion.V_Dev;
                    }
            }
        }

        throw new UnsupportedOperationException("Cannot run on Neo4j Version " + version);
    }

    public MajorMinorVersion semanticVersion() {
        if (this == V_Dev) {
            return ImmutableMajorMinorVersion.of(5, MINOR_DEV_VERSION);
        }

        String version = toString();
        var subVersions = version.split("\\.");

        if (subVersions.length < 2) {
            throw new IllegalStateException("Cannot derive version from " + version);
        }

        return ImmutableMajorMinorVersion.of(Integer.parseInt(subVersions[0]), Integer.parseInt(subVersions[1]));
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

    @Override
    public String toString() {
        switch (this) {
            case V_4_4:
                return "4.4";
            case V_5_6:
                return "5.6";
            case V_5_7:
                return "5.7";
            case V_5_8:
                return "5.8";
            case V_5_9:
                return "5.9";
            case V_5_10:
                return "5.10";
            case V_5_11:
                return "5.11";
            case V_5_12:
                return "5.12";
            case V_5_13:
                return "5.13";
            case V_5_14:
                return "5.14";
            case V_5_15:
                return "5.15";
            case V_Dev:
                return "dev";
            default:
                throw new IllegalArgumentException("Unexpected value: " + this.name() + " (sad java 😞)");
        }
    }

    @ValueClass
    public interface MajorMinorVersion {
        int major();
        int minor();

        default String asString() {
          return  major() + "." + minor();
        }
    }
}
