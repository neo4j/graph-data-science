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

-- In this example, we will use Neo4j Graph Data Science (GDS) for Snowflake to perform basket analysis on the TPC-H dataset.
-- For that we will look at parts that are often ordered together and use the node similarity algorithm to find similar parts.
-- We will then write the results back to Snowflake and query the results.
--
-- For this example to run, we make the following assumptions:
--
-- * Neo4j Graph Data Science application is installed correctly and called "Neo4j_GDS"
-- * The current role can create databases and schemas, compute pools and warehouses
-- * The current role has granted the application role Neo4j_GDS.app_user
-- * The current role has access to the Snowflake sample data set (database "snowflake_sample_data")
--
-- The example is split into three parts:
-- 1. Data preparation,
-- 2. Application setup, and
-- 3. Graph analysis.

-- ==================================================
-- 1. Data preparation
-- ==================================================

-- Create a database which we will use to prepare data for GDS.
CREATE DATABASE IF NOT EXISTS tpch_example;
CREATE SCHEMA IF NOT EXISTS tpch_example.gds;
USE SCHEMA tpch_example.gds;

-- GDS expects the data to be in a specific format: a table/view for nodes and a table/view for relationships.
-- In addition, GDS requires node identifiers to be globally unique integers.
--
-- For our analysis, the nodes will be parts and the orders in which they appeared.
-- The relationships will be the line items linking a part to an order.
--
-- We start by creating the node view for our graph.
-- First we need to map the primary keys for parts and orders to globally unique node ids.

-- We use a sequence to generate globally unique node identifiers.
CREATE OR REPLACE SEQUENCE global_id START = 0 INCREMENT = 1;

-- We create two mapping tables, one for parts and one for orders.
-- This is necessary because the primary key sets for both tables might overlap.
CREATE OR REPLACE TABLE node_mapping_parts(gdsId, p_partkey) AS
    SELECT global_id.nextval, p_partkey
    FROM snowflake_sample_data.tpch_sf1.part;
CREATE OR REPLACE TABLE node_mapping_orders(gdsId, o_orderkey) AS
    SELECT global_id.nextval, o_orderkey
    FROM snowflake_sample_data.tpch_sf1.orders;

-- Next, we can create the final node view that we use for our graph projection.
-- Note, that the view must contain a column named "nodeId" to be recognized by GDS.
-- Any additional column will be used as node property, but we don't need that for this example.
CREATE OR REPLACE VIEW nodes(nodeId) AS
    SELECT nmp.gdsId
    UNION
    SELECT nmo.gdsId;

-- Let's quickly verify the cardinality of our views.
-- As it is the union of parts and orders, we expect 1,700,000 rows.
SELECT count(*) FROM nodes;

-- We can now create the relationship view.
-- As mentioned earlier, we will use the line items to create relationships between parts and orders.
-- We join the line items with parts and orders to get the source and target nodes for our relationships.
-- We also join the mapping tables to get the globally unique node ids.
-- Note, that the view must contain columns named "sourceNodeId" and "targetNodeId" to be recognized by GDS.
-- Any additional column will be used as relationship property, but we don't need that for this example.
CREATE OR REPLACE VIEW relationships(sourceNodeId, targetNodeId) AS
    SELECT 
        nmp.gdsId AS sourceNodeId, 
        nmo.gdsId AS targetNodeId
    FROM snowflake_sample_data.tpch_sf1.part p
        -- The first two joins build the relationships between parts and orders
        JOIN snowflake_sample_data.tpch_sf1.lineitem l
          ON p.p_partkey = l.l_partkey
        JOIN snowflake_sample_data.tpch_sf1.orders o
          ON o.o_orderkey = l.l_orderkey
        -- The second two joins map the primary keys to globally unique node ids
        JOIN node_mapping_parts nmp
          ON nmp.p_partkey = p.p_partkey
        JOIN node_mapping_orders nmo
          ON nmo.o_orderkey = o.o_orderkey;

-- Let's quickly verify the cardinality of our relationship view.
-- As it is the join of parts, line items, and orders, we expect 6,001,215 rows.
SELECT count(*) FROM relationships;

-- We have now prepared the data for GDS.

-- ==================================================
-- 2. Application setup
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
-- 3. Graph analysis
-- ==================================================

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

-- ==================================================
-- 3. result analysis
-- ==================================================

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

