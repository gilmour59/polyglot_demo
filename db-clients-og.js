// --- Database Client Configurations ---
// This file centralizes the connections to your local databases.
// IMPORTANT: You may need to update these connection strings with your own credentials.

const { MongoClient } = require('mongodb');
const pg = require('pg');
const redis = require('redis');
const cassandra = require('cassandra-driver');
const neo4j = require('neo4j-driver');

// 1. MongoDB Client
const mongoUri = 'mongodb://127.0.0.1:27017/';
const mongoClient = new MongoClient(mongoUri);

// 2. PostgreSQL Client
const postgresPool = new pg.Pool({
    user: 'gilmour59',
    host: 'localhost',
    database: 'polyglot_shelf',
    password: '',
    port: 5432,
});

// 3. Redis Client
const redisClient = redis.createClient({
    url: 'redis://localhost:6379'
});

redisClient.on('error', err => console.log('Redis Client Error', err));

// --- Cassandra Client ---
// Assumes default Cassandra port and a local datacenter named 'datacenter1'
// This is the default for the official Docker image.
const cassandraClient = new cassandra.Client({
    contactPoints: ['127.0.0.1:9042'],
    localDataCenter: 'datacenter1',
    keyspace: 'polyglot_shelf'
});

// --- Neo4j Client ---
const neo4jUri = 'bolt://localhost:7687';
const neo4jUser = 'neo4j';
const neo4jPassword = '1234567890'; 
const neo4jDriver = neo4j.driver(neo4jUri, neo4j.auth.basic(neo4jUser, neo4jPassword));

module.exports = {
    mongoClient,
    postgresPool,
    redisClient,
    cassandraClient,
    neo4jDriver
};
