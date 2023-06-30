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

import java.time.ZonedDateTime;
import java.util.Optional;

class TestLicenseStates {

    static class ExpiredLicenseState implements LicenseState {
        private ZonedDateTime pastTime;

        ExpiredLicenseState(ZonedDateTime pastTime) {this.pastTime = pastTime;}

        @Override
        public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
            return visitor.invalid(
                "TestExpired",
                "License expires a long time ago.",
                Optional.of(pastTime),
                parameter
            );
        }

        @Override
        public String toString() {
            return "ExpiredLicenseState{" +
                "pastTime=" + pastTime +
                '}';
        }
    }

    static class Unlicensed implements LicenseState {
        @Override
        public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
            return visitor.unlicensed("TestUnlicensed", parameter);
        }

        @Override
        public String toString() {
            return "Unlicensed";
        }
    }

    static class Invalid implements LicenseState {
        @Override
        public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
            return visitor.invalid(
                "TestInvalid",
                "License invalid for some reason.",
                Optional.empty(),
                parameter
            );
        }

        @Override
        public String toString() {
            return "Invalid";
        }
    }

    static class Valid implements LicenseState {
        private final ZonedDateTime futureTime;

        Valid(ZonedDateTime futureTime) {this.futureTime = futureTime;}

        @Override
        public <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter) {
            return visitor.licensed("TestValid", futureTime, parameter);
        }

        @Override
        public String toString() {
            return "Valid{" +
                "futureTime=" + futureTime +
                '}';
        }
    }
}
