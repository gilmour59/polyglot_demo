// seed.js
// This script populates all five databases with initial data.
// Run it once from your terminal: `node seed.js`

const { postgresPool, mongoClient, redisClient, neo4jDriver } = require('./db-clients');
const cassandra = require('cassandra-driver');

// --- Sample Data ---
const sampleProducts = [
    { _id: "prod_1", title: "Designing Data-Intensive Applications", author: "Martin Kleppmann", price: 45.99, imageUrl: "https://placehold.co/600x400/3273dc/ffffff?text=DDI" },
    { _id: "prod_2", title: "Building Microservices", author: "Sam Newman", price: 39.99, imageUrl: "https://placehold.co/600x400/23d160/ffffff?text=Microservices" },
    { _id: "prod_3", title: "Fundamentals of Data Engineering", author: "Joe Reis & Matt Housley", price: 55.00, imageUrl: "https://placehold.co/600x400/ffdd57/000000?text=FDE" }
];
const sampleUser = { id: 'user-123-demo', name: 'Demo User' };

// --- Seeding Functions ---

async function seedPostgres() {
    console.log('Seeding PostgreSQL...');
    await postgresPool.query('DROP TABLE IF EXISTS users;');
    await postgresPool.query(`
        CREATE TABLE users (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    `);
    await postgresPool.query('INSERT INTO users (id, name) VALUES ($1, $2)', [sampleUser.id, sampleUser.name]);
    console.log('  - "users" table created and seeded.\n');
}

async function seedMongo() {
    console.log('Seeding MongoDB...');
    const db = mongoClient.db('polyglot_shelf');
    await db.collection('products').deleteMany({});
    await db.collection('products').insertMany(sampleProducts);
    console.log(`  - "products" collection seeded with ${sampleProducts.length} items.\n`);
}

async function seedRedis() {
    console.log('Seeding Redis...');
    await redisClient.del(`cart:${sampleUser.id}`);
    console.log('  - Cleared stale cart data.\n');
}

async function seedCassandra() {
    console.log('Seeding Cassandra...');
    // Special client to connect *without* a keyspace to create it first.
    const tempClient = new cassandra.Client({
        contactPoints: ['127.0.0.1'],
        localDataCenter: 'datacenter1',
        protocolOptions: { port: 9042 }
    });

    try {
        await tempClient.connect();
        // Use a simple strategy for a single-node dev setup
        await tempClient.execute(`
            CREATE KEYSPACE IF NOT EXISTS polyglot_shelf 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`
        );
        console.log('  - "polyglot_shelf" keyspace ensured.');

        await tempClient.execute(`
            CREATE TABLE IF NOT EXISTS polyglot_shelf.reviews (
                product_id TEXT,
                review_id TIMEUUID,
                user_name TEXT,
                text TEXT,
                PRIMARY KEY (product_id, review_id)
            ) WITH CLUSTERING ORDER BY (review_id DESC);
        `);
        console.log('  - "reviews" table ensured.');

    } finally {
        await tempClient.shutdown();
    }
    console.log('Cassandra seeding complete.\n');
}

async function seedNeo4j() {
    console.log('Seeding Neo4j...');
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        await session.run('MATCH (n) DETACH DELETE n'); // Clear the database
        console.log('  - Cleared existing graph data.');

        await session.run('CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE');
        await session.run('CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE');
        console.log('  - Ensured constraints on User and Product nodes.');
        
        // Create the user node
        await session.run('CREATE (u:User {id: $id, name: $name})', sampleUser);

        // Create product nodes
        for (const product of sampleProducts) {
             await session.run('CREATE (p:Product {id: $id, title: $title})', { id: product._id, title: product.title });
        }
        console.log('  - Created User and Product nodes.');

    } finally {
        await session.close();
    }
    console.log('Neo4j seeding complete.\n');
}

async function main() {
    try {
        await mongoClient.connect();
        await redisClient.connect();
        console.log('Connected to ancillary databases.\n');

        await seedPostgres();
        await seedMongo();
        await seedRedis();
        await seedCassandra();
        await seedNeo4j();

        console.log('✅ All databases have been successfully seeded!');

    } catch (error) {
        console.error('🔥 An error occurred during seeding:', error);
    } finally {
        await postgresPool.end();
        await mongoClient.close();
        await redisClient.quit();
        await neo4jDriver.close();
        console.log('\nAll database clients disconnected.');
    }
}

main();

