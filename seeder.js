// seed.js
// This script populates the databases with initial data.
// Run it once from your terminal: `node seed.js`

const cassandra = require('cassandra-driver');
const { mongoClient, postgresPool, redisClient, cassandraClient, neo4jDriver } = require('./db-clients');

// --- 1. MongoDB: Demonstrating Flexibility ---
// Notice the 4th book has two extra fields: `series_name` and `series_number`.
// MongoDB handles this flexible schema without any issues.
const sampleProducts = [
    { _id: "prod_1", title: "Designing Data-Intensive Applications", author: "Martin Kleppmann", price: 45.99, imageUrl: "https://placehold.co/600x400/3273dc/ffffff?text=DDI" },
    { _id: "prod_2", title: "Building Microservices", author: "Sam Newman", price: 39.99, imageUrl: "https://placehold.co/600x400/23d160/ffffff?text=Microservices" },
    { _id: "prod_3", title: "Fundamentals of Data Engineering", author: "Joe Reis & Matt Housley", price: 55.00, imageUrl: "https://placehold.co/600x400/ffdd57/000000?text=FDE" },
    { _id: "prod_4", title: "Clean Architecture", author: "Robert C. Martin", price: 35.50, imageUrl: "https://placehold.co/600x400/f14668/ffffff?text=Clean+Arch", series_name: "Robert C. Martin Series", series_number: 1 }
];

// --- 2. Cassandra: Demonstrating Denormalization ---
// We are intentionally storing `user_name` here. In a relational model, you'd only store
// `user_id` and perform a JOIN. Here, we duplicate the name to make reads faster.
const sampleReviews = {
    "prod_1": [ { productId: "prod_1", userName: "Alex", text: "A must-read for any software engineer." } ],
    "prod_2": [ { productId: "prod_2", userName: "Brenda", text: "Great practical advice." } ]
};

// --- 3. Neo4j: The initial state of our graph ---
// We create some initial purchase relationships to power the recommendation engine.
const samplePurchases = [
    { userId: 'user-alex-demo', productId: 'prod_1' },
    { userId: 'user-alex-demo', productId: 'prod_3' }, // Alex bought prod_1 and prod_3
    { userId: 'user-brenda-demo', productId: 'prod_1' },
    { userId: 'user-brenda-demo', productId: 'prod_3' }, // Brenda also bought prod_1 and prod_3
    { userId: 'user-chris-demo', productId: 'prod_2' },
];


async function seedPostgres() {
    console.log('Seeding PostgreSQL...');
    await postgresPool.query(`DROP TABLE IF EXISTS users;`);
    await postgresPool.query(`
        CREATE TABLE users (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    `);
    console.log('  - "users" table created.');

    const initialUsers = [
        ['user-alex-demo', 'Alex'],
        ['user-brenda-demo', 'Brenda'],
        ['user-chris-demo', 'Chris']
    ];
    for (const user of initialUsers) {
        await postgresPool.query('INSERT INTO users (id, name) VALUES ($1, $2)', user);
    }
    console.log(`  - Inserted ${initialUsers.length} initial users.`);
    console.log('PostgreSQL seeding complete.\n');
}

async function seedMongo() {
    console.log('Seeding MongoDB...');
    const db = mongoClient.db('polyglot_shelf');
    await db.collection('products').deleteMany({});
    await db.collection('products').insertMany(sampleProducts);
    console.log(`  - Inserted ${sampleProducts.length} products (including one with a flexible schema).`);
    console.log('MongoDB seeding complete.\n');
}

async function seedCassandra() {
    console.log('Seeding Cassandra...');
    const tempClient = new cassandra.Client({
        contactPoints: ['127.0.0.1'],
        localDataCenter: 'datacenter1',
        credentials: { username: 'cassandra', password: 'cassandra' }
    });
    
    // Create the keyspace if it doesn't exist
    await tempClient.execute(`
        CREATE KEYSPACE IF NOT EXISTS polyglot_shelf 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }
    `);
    await tempClient.shutdown();
    console.log('  - "polyglot_shelf" keyspace ensured.');

    // Drop the table first to ensure a clean schema on every run
    await cassandraClient.execute(`DROP TABLE IF EXISTS polyglot_shelf.reviews`);

    // Now (re)create the table with the correct schema
    await cassandraClient.execute(`
        CREATE TABLE polyglot_shelf.reviews (
            product_id TEXT,
            review_id UUID,
            user_name TEXT,
            text TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (product_id, created_at)
        ) WITH CLUSTERING ORDER BY (created_at DESC);
    `);
    console.log('  - "reviews" table ensured.');
    
    // Clear the table and insert new data
    await cassandraClient.execute('TRUNCATE polyglot_shelf.reviews');
    const allReviews = Object.values(sampleReviews).flat();
    for (const review of allReviews) {
        const query = 'INSERT INTO polyglot_shelf.reviews (product_id, review_id, user_name, text, created_at) VALUES (?, uuid(), ?, ?, toTimestamp(now()))';
        await cassandraClient.execute(query, [review.productId, review.userName, review.text], { prepare: true });
    }
    console.log(`  - Inserted ${allReviews.length} reviews.`);
    console.log('Cassandra seeding complete.\n');
}


async function seedRedis() {
    console.log('Seeding Redis...');
    const keys = await redisClient.keys('cart:*');
    if (keys.length > 0) {
        await redisClient.del(keys);
        console.log(`  - Cleared ${keys.length} stale cart(s).`);
    } else {
        console.log('  - No stale carts to clear.');
    }
    console.log('Redis seeding complete.\n');
}

async function seedNeo4j() {
    console.log('Seeding Neo4j...');
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        await session.run('MATCH (n) DETACH DELETE n');
        console.log('  - Cleared existing graph data.');
        
        // Create product nodes
        const productIds = sampleProducts.map(p => p._id);
        await session.run(
            `UNWIND $productIds AS pId MERGE (p:Product {id: pId})`,
            { productIds }
        );
        console.log(`  - Created ${productIds.length} product nodes.`);

        // Create user nodes and purchase relationships
        await session.run(
            `
            UNWIND $purchases AS purchase
            MERGE (u:User {id: purchase.userId})
            MERGE (p:Product {id: purchase.productId})
            MERGE (u)-[:PURCHASED]->(p)
            `,
            { purchases: samplePurchases }
        );
        console.log(`  - Created ${samplePurchases.length} purchase relationships.`);

    } finally {
        await session.close();
    }
    console.log('Neo4j seeding complete.\n');
}


async function main() {
    try {
        await mongoClient.connect();
        await redisClient.connect();
        // Cassandra and Neo4j connect inside their seed functions
        console.log('Mongo and Redis clients connected successfully.\n');

        await seedPostgres();
        await seedMongo();
        await seedCassandra();
        await seedRedis();
        await seedNeo4j();

        console.log('✅ All databases have been successfully seeded!');

    } catch (error) {
        console.error('🔥 An error occurred during seeding:', error);
    } finally {
        await postgresPool.end();
        await mongoClient.close();
        await redisClient.quit();
        await cassandraClient.shutdown();
        await neo4jDriver.close();
        console.log('\nAll database clients disconnected.');
    }
}

main();


