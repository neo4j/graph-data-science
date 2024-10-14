/*
Copyright (c) "Neo4j"
Neo4j Sweden AB [http://neo4j.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/


-- *========================================*
-- | Neo4j Graph Data Science for Snowflake |
-- | Basket analysis example on TPC-H data  |
-- *========================================*

-- Assumptions:
-- * Neo4j Graph Data Science application is installed correctly and called "Neo4j_GDS"
-- * The current role can create databases and schemas, compute pools and warehouses
-- * The current role has granted the application role Neo4j_GDS.app_user


-- Create a database which we will use to prepare data for GDS
-- and also to write back graph algorithm results.
CREATE DATABASE IF NOT EXISTS tpch_example;
CREATE SCHEMA IF NOT EXISTS tpch_example.gds;
USE SCHEMA tpch_example.gds;


-- ====
-- Create mapping tables for each table we want to project as nodes.
-- We map the primary keys of each table to a globally unique id.
-- ====

-- We use a sequence to generate globally unqiue node ids
CREATE OR REPLACE SEQUENCE seq1 START = 0 INCREMENT = 1;

-- Map part keys to globally unique node ids.
-- We create tables to persist the generated ids.
CREATE OR REPLACE TABLE node_mapping_parts(gdsId, p_partkey) AS
    SELECT seq1.nextval, p_partkey
    FROM snowflake_sample_data.tpch_sf1.part;
-- map order keys to globally unique node ids
CREATE OR REPLACE TABLE node_mapping_orders(gdsId, o_orderkey) AS
    SELECT seq1.nextval, o_orderkey
    FROM snowflake_sample_data.tpch_sf1.orders;

--
-- Build view using the data we want to project as relationships.
-- We join in the mapping information we created beforehand.
--

-- map (:Part)-[:IN]->(:Order)
CREATE OR REPLACE VIEW relationships(sourceNodeId, targetNodeId) AS
    SELECT 
        nmp.gdsId AS sourceNodeId, 
        nmo.gdsId AS targetNodeId
    FROM snowflake_sample_data.tpch_sf1.part p 
        JOIN snowflake_sample_data.tpch_sf1.lineitem l
          ON p.p_partkey = l.l_partkey
        JOIN snowflake_sample_data.tpch_sf1.orders o
          ON o.o_orderkey = l.l_orderkey
        -- we join in the globally unique node ids to
        -- match the node table
        JOIN node_mapping_parts nmp
          ON nmp.p_partkey = p.p_partkey
        JOIN node_mapping_orders nmo
          ON nmo.o_orderkey = o.o_orderkey;

--
-- Build a node view that contains all nodes that have outgoing or
-- incoming relationships.
--
        
CREATE OR REPLACE VIEW nodes(nodeId) AS 
    SELECT nmp.gdsId 
    FROM node_mapping_parts nmp
        JOIN relationships r 
          ON nmp.gdsId = r.sourceNodeId
    UNION
    SELECT nmo.gdsId 
    FROM node_mapping_orders nmo
        JOIN relationships r 
          ON nmo.gdsId = r.targetNodeId;

-- Verify the cardinality of our views

-- Node view: 1,700,000 rows
SELECT count(*) FROM nodes;
-- Relationship view: 6,001,215 rows
SELECT count(*) FROM relationships;


-- ==================================================


-- ====
-- Setup compute pool and application
-- ====
USE DATABASE Neo4j_GDS;

-- We create a compute pool to execute the service on
CREATE COMPUTE POOL IF NOT EXISTS Neo4j_GDS_pool
  FOR APPLICATION Neo4j_GDS
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_L
  AUTO_RESUME = true;
GRANT USAGE ON COMPUTE POOL Neo4j_GDS_pool TO APPLICATION Neo4j_GDS;

CREATE WAREHOUSE IF NOT EXISTS Neo4j_GDS_warehouse
  WAREHOUSE_SIZE='MEDIUM'
  AUTO_SUSPEND = 180
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = false;
GRANT USAGE ON WAREHOUSE Neo4j_GDS_warehouse TO APPLICATION Neo4j_GDS;

GRANT USAGE ON WAREHOUSE Neo4j_GDS_warehouse TO APPLICATION Neo4j_GDS;
GRANT USAGE ON DATABASE  tpch_example        TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA    tpch_example.gds    TO APPLICATION Neo4j_GDS;
GRANT SELECT ON ALL TABLES IN SCHEMA tpch_example.gds TO APPLICATION Neo4j_GDS;
GRANT SELECT ON ALL VIEWS  IN SCHEMA tpch_example.gds TO APPLICATION Neo4j_GDS;
GRANT CREATE TABLE ON SCHEMA tpch_example.gds TO APPLICATION Neo4j_GDS;



-- ==================================================


-- ====
-- Use GDS for basket analysis.
-- ====

-- Create a new GDS session
CALL gds.create_session('Neo4j_GDS_pool', 'Neo4j_GDS_warehouse');

-- Project node and relationship view into GDS graph
SELECT gds.graph_project('parts_in_orders', {
    'nodeTable':         'tpch_example.gds.nodes',
    'relationshipTable': 'tpch_example.gds.relationships',
    'readConcurrency':   28
});

-- Run node similarity and produce new relationships to represent similarity
-- This takes about ten minutes
SELECT gds.node_similarity('parts_in_orders', {
    'mutateRelationshipType': 'SIMILAR_TO',
    'mutateProperty':         'similarity',
    'concurrency':            28
});

-- Write relationships back to Snowflake
SELECT gds.write_relationships('parts_in_orders', {
    'relationshipType':     'SIMILAR_TO',
    'relationshipProperty': 'similarity',
    'table':                'tpch_example.gds.part_similar_to_part'
});

-- Grant select privilege to your role on the new table create by GDS

GRANT SELECT ON tpch_example.gds.part_similar_to_part TO ROLE <your_role>; 

-- Select most similar parts based on the algorithm result.
SELECT DISTINCT p_source.p_name, p_target.p_name, sim.similarity
FROM snowflake_sample_data.tpch_sf1.part p_source
JOIN tpch_example.gds.node_mapping_parts nmp_source
  ON p_source.p_partkey = nmp_source.p_partkey
JOIN tpch_example.gds.part_similar_to_part sim
  ON nmp_source.gdsid = sim.sourcenodeid
JOIN tpch_example.gds.node_mapping_parts nmp_target
  ON sim.targetnodeid = nmp_target.gdsid
JOIN snowflake_sample_data.tpch_sf1.part p_target
  ON nmp_target.p_partkey = p_target.p_partkey
ORDER BY sim.similarity DESC
LIMIT 10;

-- Stop the GDS session
CALL Neo4j_GDS.gds.stop_session();

