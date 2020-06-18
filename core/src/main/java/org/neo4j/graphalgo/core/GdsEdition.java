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

public final class GdsEdition {

    private static final GdsEdition INSTANCE = new GdsEdition();

    public static GdsEdition instance() {
        return INSTANCE;
    }

    private enum State {
        ENTERPRISE,
        COMMUNITY
    }

    private State currentState;

    private GdsEdition() {
        this.currentState = State.COMMUNITY;
    }

    public boolean isOnEnterpriseEdition() {
        return get() == State.ENTERPRISE;
    }

    public boolean isOnCommunityEdition() {
        return !isOnEnterpriseEdition();
    }

    public void setToEnterpriseEdition() {
        set(State.ENTERPRISE);
    }

    public void setToCommunityEdition() {
        set(State.COMMUNITY);
    }

    private void set(State state) {
        this.currentState = state;
    }

    private State get() {
        return currentState;
    }
}
