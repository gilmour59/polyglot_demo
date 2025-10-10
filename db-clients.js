// db-clients.js
// This file centralizes the connections to all our different databases.

const pg = require('pg');
const { MongoClient } = require('mongodb');
const redis = require('redis');
const cassandra = require('cassandra-driver');
const neo4j = require('neo4j-driver');

// --- IMPORTANT: CONFIGURE YOUR CREDENTIALS HERE ---

// 1. PostgreSQL Client
// Replace 'myuser' with your actual macOS username if you are not using the default 'postgres' role.
// Replace 'YOUR_POSTGRES_PASSWORD' with your PostgreSQL password.
const postgresPool = new pg.Pool({
    user: 'gilmour59',
    host: 'localhost',
    database: 'polyglot_shelf',
    password: '',
    port: 5432,
});

// 2. MongoDB Client
const mongoClient = new MongoClient('mongodb://localhost:27017');

// 3. Redis Client
const redisClient = redis.createClient({
    url: 'redis://localhost:6379'
});
redisClient.on('error', err => console.error('Redis Client Error', err));

// 4. Cassandra Client
// This client connects to the keyspace *after* it has been created by the seeder.
const cassandraClient = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    localDataCenter: 'datacenter1', // Default for Docker images
    keyspace: 'polyglot_shelf',
    protocolOptions: { port: 9042 }
});

// 5. Neo4j Driver
// Replace 'YOUR_NEO4J_PASSWORD' with the password you set when starting the Neo4j Docker container.
const neo4jDriver = neo4j.driver(
    'bolt://localhost:7687',
    neo4j.auth.basic('neo4j', '1234567890')
);

module.exports = {
    postgresPool,
    mongoClient,
    redisClient,
    cassandraClient,
    neo4jDriver
};

