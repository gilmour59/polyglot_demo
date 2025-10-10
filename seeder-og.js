// seed.js
const { mongoClient, postgresPool, redisClient, cassandraClient, neo4jDriver } = require('./db-clients');
const cassandra = require('cassandra-driver'); // Import driver for bootstrap client

const sampleProducts = [
    {
        _id: "prod_1",
        title: "Designing Data-Intensive Applications",
        author: "Martin Kleppmann",
        price: 45.99,
        imageUrl: "https://placehold.co/600x400/3273dc/ffffff?text=DDI"
    },
    {
        _id: "prod_2",
        title: "Building Microservices",
        author: "Sam Newman",
        price: 39.99,
        imageUrl: "https://placehold.co/600x400/23d160/ffffff?text=Microservices"
    },
    {
        _id: "prod_3",
        title: "Fundamentals of Data Engineering",
        author: "Joe Reis & Matt Housley",
        price: 55.00,
        imageUrl: "https://placehold.co/600x400/ffdd57/000000?text=FDE"
    }
];

const sampleReviews = {
    "prod_1": [
        { productId: "prod_1", userName: "Alex", text: "A must-read for any software engineer. Incredibly dense and informative." },
        { productId: "prod_1", userName: "Maria", text: "Changed the way I think about systems." }
    ],
    "prod_2": [
        { productId: "prod_2", userName: "Chris", text: "Great practical advice for moving to a microservices architecture." }
    ]
};


// --- Seeding Functions for each Database ---

async function seedPostgres() {
    console.log('Seeding PostgreSQL...');
    await postgresPool.query(`
        DROP TABLE IF EXISTS users;
        CREATE TABLE users (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    `);
    console.log('  - "users" table created.');
    await postgresPool.query(
        'INSERT INTO users (id, name) VALUES ($1, $2)',
        ['user-123-demo', 'Demo User']
    );
    console.log('  - Sample user inserted.');
    console.log('PostgreSQL seeding complete.\n');
}

async function seedMongo() {
    console.log('Seeding MongoDB...');
    const db = mongoClient.db('polyglot_shelf');
    await db.collection('products').deleteMany({});
    await db.collection('products').insertMany(sampleProducts);
    console.log(`  - Inserted ${sampleProducts.length} products.`);
    console.log('MongoDB seeding complete.\n');
}

async function seedRedis() {
    console.log('Seeding Redis...');
    await redisClient.del('cart:user-123-demo');
    console.log('  - Cleared any stale cart data.');
    console.log('Redis seeding complete.\n');
}

async function seedCassandra() {
    console.log('Seeding Cassandra...');
    // Step 1: Create a temporary client without a keyspace to perform bootstrap operations.
    const bootstrapClient = new cassandra.Client({
        contactPoints: ['127.0.0.1:9042'],
        localDataCenter: 'datacenter1',
        // NO 'keyspace' property here.
    });

    // Step 2: Create the keyspace using the bootstrap client.
    await bootstrapClient.execute(`
        CREATE KEYSPACE IF NOT EXISTS polyglot_shelf 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    `);
    console.log('  - Keyspace "polyglot_shelf" ensured.');
    // Done with the bootstrap client, so we shut it down.
    await bootstrapClient.shutdown();

    // Step 3: Now that the keyspace exists, our main shared client (cassandraClient) can be used.
    // It will connect to the 'polyglot_shelf' keyspace as configured in db-clients.js.
    await cassandraClient.execute(`
        CREATE TABLE IF NOT EXISTS polyglot_shelf.reviews (
            product_id text,
            review_id timeuuid,
            user_name text,
            text text,
            PRIMARY KEY (product_id, review_id)
        );
    `);
    console.log('  - Table "reviews" ensured.');

    // Step 4: Insert reviews using the main client.
    const allReviews = Object.values(sampleReviews).flat();
    const query = 'INSERT INTO polyglot_shelf.reviews (product_id, review_id, user_name, text) VALUES (?, now(), ?, ?)';
    for (const review of allReviews) {
        await cassandraClient.execute(query, [review.productId, review.userName, review.text], { prepare: true });
    }
    console.log(`  - Inserted ${allReviews.length} reviews.`);
    console.log('Cassandra seeding complete.\n');
}

async function seedNeo4j() {
    console.log('Seeding Neo4j...');
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        await session.run('MATCH (n) DETACH DELETE n');
        console.log('  - Cleared existing graph data.');

        for (const product of sampleProducts) {
            await session.run('CREATE (:Product {id: $id, title: $title})', { id: product._id, title: product.title });
        }
        console.log(`  - Created ${sampleProducts.length} product nodes.`);

        await session.run("CREATE (:User {id: 'user-A', name: 'Alice'})");
        await session.run("CREATE (:User {id: 'user-B', name: 'Bob'})");

        await session.run(`
            MATCH (u:User {id: 'user-A'}), (p:Product {id: 'prod_1'}) CREATE (u)-[:PURCHASED]->(p)
        `);
        await session.run(`
            MATCH (u:User {id: 'user-A'}), (p:Product {id: 'prod_2'}) CREATE (u)-[:PURCHASED]->(p)
        `);
        await session.run(`
            MATCH (u:User {id: 'user-B'}), (p:Product {id: 'prod_2'}) CREATE (u)-[:PURCHASED]->(p)
        `);
        await session.run(`
            MATCH (u:User {id: 'user-B'}), (p:Product {id: 'prod_3'}) CREATE (u)-[:PURCHASED]->(p)
        `);
        console.log('  - Created users and purchase relationships.');
    } finally {
        await session.close();
    }
    console.log('Neo4j seeding complete.\n');
}


async function main() {
    try {
        await mongoClient.connect();
        await redisClient.connect();
        // The main cassandra client will connect on its first query inside seedCassandra.

        console.log('Clients ready.\n');

        await seedPostgres();
        await seedMongo();
        await seedRedis();
        await seedCassandra(); // New and improved
        await seedNeo4j();     // New

        console.log('✅ All databases have been successfully seeded!');

    } catch (error) {
        console.error('🔥 An error occurred during seeding:', error);
    } finally {
        // Ensure all clients are gracefully closed.
        await postgresPool.end();
        await mongoClient.close();
        await redisClient.quit();
        await cassandraClient.shutdown();
        await neo4jDriver.close();
        console.log('\nAll database clients disconnected.');
    }
}

main();

