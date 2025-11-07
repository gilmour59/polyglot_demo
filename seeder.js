// seed.js
// This script populates the databases with initial data.
// Run it once from your terminal: `node seed.js`

const cassandra = require('cassandra-driver');
const { mongoClient, postgresPool, redisClient, cassandraClient, neo4jDriver } = require('./db-clients');

// --- 1. MongoDB: Demonstrating Flexibility ---
// Updated to 20 book titles, all "deez nuts" jokes.
const sampleProducts = [
    { _id: "dn_prod_1", title: "The Grapes of Sawkon", author: "Jon Steinbofades", price: 19.99, imageUrl: "https://placehold.co/600x400/3273dc/ffffff?text=Sawkon" },
    { _id: "dn_prod_2", title: "Lord of the Deez: The Fellowship of the Nuts", author: "J.R.R. Holden", price: 22.99, imageUrl: "https://placehold.co/600x400/23d160/ffffff?text=Lord+of+Deez" },
    { _id: "dn_prod_3", title: "A Tale of Two Deez: A Bofades Story", author: "Charles Dickens", price: 18.99, imageUrl: "https://placehold.co/600x400/ffdd57/000000?text=Two+Deez" },
    { _id: "dn_prod_4", title: "The Great Got-Eem: A Sugondese Tragedy", author: "F. Scott Phitzgerald", price: 21.50, imageUrl: "https://placehold.co/600x400/f14668/ffffff?text=Got-Eem" },
    { _id: "dn_prod_5", title: "To Kill a Sugma Bird", author: "Harper Lee-gma", price: 14.99, imageUrl: "https://placehold.co/600x400/b86bff/ffffff?text=Sugma+Bird" },
    { _id: "dn_prod_6", title: "Pride and Bofades", author: "Jane Hausten", price: 12.95, imageUrl: "https://placehold.co/600x400/ff6b6b/ffffff?text=Bofades" },
    { _id: "dn_prod_7", title: "1984: The Year of Deez", author: "George R. Well", price: 17.00, imageUrl: "https://placehold.co/600x400/48c774/ffffff?text=1984" },
    { _id: "dn_prod_8", title: "The Catcher in the Nuts", author: "J.D. Salingon", price: 16.50, imageUrl: "https://placehold.co/600x400/3298dc/ffffff?text=Catcher" },
    { _id: "dn_prod_9", title: "Moby Deez; or, The Nut", author: "Herman Mel-vil", price: 24.00, imageUrl: "https://placehold.co/600x400/dc3232/ffffff?text=Moby+Deez" },
    { _id: "dn_prod_10", title: "Don Deez-ote", author: "Miguel deez Cervantes", price: 28.10, imageUrl: "https://placehold.co/600x400/ff9900/ffffff?text=Don+Deez" },
    { _id: "dn_prod_11", title: "War and Deez", author: "Leo Toll-stoy", price: 33.45, imageUrl: "https://placehold.co/600x400/00d1b2/ffffff?text=War" },
    { _id: "dn_prod_12", title: "The Adventures of Huckleberry Phitt", author: "Mark Twain-uts", price: 15.99, imageUrl: "https://placehold.co/600x400/6f42c1/ffffff?text=Huck+Phitt" },
    { _id: "dn_prod_13", title: "Franken-knuts", author: "Mary Shelly", price: 22.00, imageUrl: "https://placehold.co/600x400/fbc02d/000000?text=Franken-knuts", series_name: "Gothic Deez", series_number: 1 },
    { _id: "dn_prod_14", title: "Dracula: A Bofades Tale", author: "Bram Stoker", price: 18.25, imageUrl: "https://placehold.co/600x400/17a2b8/ffffff?text=Dracula", series_name: "Gothic Deez", series_number: 2 },
    { _id: "dn_prod_15", title: "The Sugondese Nuts: A Study", author: "Dr. Wilma Knuts", price: 9.99, imageUrl: "https://placehold.co/600x400/fd7e14/ffffff?text=Sugondese" },
    { _id: "dn_prod_16", title: "Ligma: The Unseen Force", author: "Dr. Howard Z. Knuts", price: 47.80, imageUrl: "https://placehold.co/600x400/e83e8c/ffffff?text=Ligma" },
    { _id: "dn_prod_17", title: "The Candice Prophecy", author: "Prof. Axelrod", price: 53.00, imageUrl: "https://placehold.co/600x400/28a745/ffffff?text=Candice" },
    { _id: "dn_prod_18", title: "Journey to the Center of Deez", author: "Jules Verne-uts", price: 23.00, imageUrl: "https://placehold.co/600x400/dc3545/ffffff?text=Journey" },
    { _id: "dn_prod_19", title: "The Art of Sawkon", author: "Sun Tzu-gma", price: 28.75, imageUrl: "https://placehold.co/600x400/6610f2/ffffff?text=Sawkon" },
    { _id: "dn_prod_20", title: "Deez Nuts: An Autobiography", author: "A. L. Mentary", price: 31.00, imageUrl: "https://placehold.co/600x400/20c997/ffffff?text=Autobiography", series_name: "Nut Studies", series_number: 1 }
];

// --- 2. Cassandra: Demonstrating Denormalization ---
const sampleReviews = {
    "dn_prod_1": [ { productId: "dn_prod_1", userName: "Alex", text: "Couldn't put it down. A real handful." } ],
    "dn_prod_2": [ { productId: "dn_prod_2", userName: "Brenda", text: "One does not simply walk into Sawkon." } ],
    "dn_prod_10": [ { productId: "dn_prod_10", userName: "Wendy", text: "Got 'em! The book was also surprisingly informative." } ],
    "dn_prod_16": [ { productId: "dn_prod_16", userName: "Mike", text: "A truly mind-boggling condition. Frightening." } ]
};

// --- 3. Neo4j: The initial state of our graph ---
const samplePurchases = [
    { userId: 'user-alex-demo', productId: 'dn_prod_1' },
    { userId: 'user-alex-demo', productId: 'dn_prod_9' },
    { userId: 'user-brenda-demo', productId: 'dn_prod_1' },
    { userId: 'user-brenda-demo', productId: 'dn_prod_2' },
    { userId: 'user-chris-demo', productId: 'dn_prod_5' },
    { userId: 'user-dee-demo', productId: 'dn_prod_15' },
    { userId: 'user-dee-demo', productId: 'dn_prod_16' },
    { userId: 'user-dee-demo', productId: 'dn_prod_17' },
    { userId: 'user-mike-demo', productId: 'dn_prod_12' },
    { userId: 'user-mike-demo', productId: 'dn_prod_16' },
    { userId: 'user-phil-demo', productId: 'dn_prod_19' },
    { userId: 'user-phil-demo', productId: 'dn_prod_6' },
    { userId: 'user-wendy-demo', productId: 'dn_prod_10' },
    { userId: 'user-wendy-demo', productId: 'dn_prod_13' },
    { userId: 'user-wendy-demo', productId: 'dn_prod_8' },
    { userId: 'user-ben-demo', productId: 'dn_prod_18' }
];

async function seedPostgres() {
    console.log('Seeding PostgreSQL...');
    await postgresPool.query(`DROP TABLE IF EXISTS users;`);
    // Re-create the users table
    await postgresPool.query(`
        CREATE TABLE users (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    `);
    console.log('  - "users" table created.');

    // Added new demo users
    const initialUsers = [
        ['user-alex-demo', 'Alex'],
        ['user-brenda-demo', 'Brenda'],
        ['user-chris-demo', 'Chris'],
        ['user-dee-demo', 'Dee'],
        ['user-mike-demo', 'Mike'],
        ['user-phil-demo', 'Phil'],
        ['user-ben-demo', 'Ben'],
        ['user-wendy-demo', 'Wendy']
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
    console.log(`  - Inserted ${sampleProducts.length} products (including flexible schema).`);
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
    
    // Insert new data
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