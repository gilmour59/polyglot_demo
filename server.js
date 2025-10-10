// server.js
const express = require('express');
const cors = require('cors');
const { mongoClient, postgresPool, redisClient, cassandraClient, neo4jDriver } = require('./db-clients');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = 3000;

// --- API Endpoints ---

// 1. Get or Create User (from PostgreSQL)
app.post('/api/users/find-or-create', async (req, res) => {
    try {
        const { id, name } = req.body;
        if (!id || !name) {
            return res.status(400).json({ error: 'User ID and name are required' });
        }

        // Check if user exists
        let result = await postgresPool.query('SELECT * FROM users WHERE id = $1', [id]);
        
        // If user does not exist, create them
        if (result.rows.length === 0) {
            await postgresPool.query(
                'INSERT INTO users (id, name) VALUES ($1, $2)',
                [id, name]
            );
            // Fetch the newly created user to return it
            result = await postgresPool.query('SELECT * FROM users WHERE id = $1', [id]);
        }
        
        res.status(200).json(result.rows[0]);
    } catch (err) {
        console.error('Error finding or creating user in PostgreSQL:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});


// 2. Get Products (from MongoDB)
app.get('/api/products', async (req, res) => {
    try {
        const db = mongoClient.db('polyglot_shelf');
        const productsFromDb = await db.collection('products').find({}).toArray();
        
        const products = productsFromDb.map(p => ({
            id: p._id,
            title: p.title,
            author: p.author,
            price: p.price,
            imageUrl: p.imageUrl
        }));

        res.json(products);
    } catch (err) {
        console.error('Error fetching products from MongoDB:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 3. Get/Update Cart (from Redis)
app.get('/api/cart/:userId', async (req, res) => {
    try {
        const { userId } = req.params;
        const cartJson = await redisClient.get(`cart:${userId}`);
        const cart = cartJson ? JSON.parse(cartJson) : { items: {} };
        res.json(cart);
    } catch (err) {
        console.error('Error fetching cart from Redis:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.post('/api/cart/:userId', async (req, res) => {
    try {
        const { userId } = req.params;
        const { productId, title, price } = req.body;
        const cartKey = `cart:${userId}`;
        
        const cartJson = await redisClient.get(cartKey);
        const cart = cartJson ? JSON.parse(cartJson) : { items: {} };

        if (cart.items[productId]) {
            cart.items[productId].quantity++;
        } else {
            cart.items[productId] = { title, price, quantity: 1 };
        }

        await redisClient.set(cartKey, JSON.stringify(cart));
        res.status(200).json(cart);
    } catch (err) {
        console.error('Error updating cart in Redis:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 4. Get/Create Reviews (from Cassandra)
app.get('/api/reviews/:productId', async (req, res) => {
    try {
        const { productId } = req.params;
        const query = 'SELECT * FROM polyglot_shelf.reviews WHERE product_id = ?';
        const result = await cassandraClient.execute(query, [productId], { prepare: true });
        res.json(result.rows);
    } catch (err) {
        console.error('Error fetching reviews from Cassandra:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.post('/api/reviews/:productId', async (req, res) => {
    try {
        const { productId } = req.params;
        const { userName, text } = req.body;
        const query = 'INSERT INTO polyglot_shelf.reviews (product_id, review_id, user_name, text, created_at) VALUES (?, uuid(), ?, ?, toTimestamp(now()))';
        await cassandraClient.execute(query, [productId, userName, text], { prepare: true });
        res.status(201).send(); // Send 201 Created with no body
    } catch (err) {
        console.error('Error creating review in Cassandra:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});


// 5. Checkout (Updates Redis and Neo4j)
app.post('/api/checkout/:userId', async (req, res) => {
    const { userId } = req.params;
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        // 1. Get cart from Redis
        const cartKey = `cart:${userId}`;
        const cartJson = await redisClient.get(cartKey);
        if (!cartJson) {
            return res.status(404).json({ error: 'Cart not found or is empty' });
        }
        const cart = JSON.parse(cartJson);
        const productIds = Object.keys(cart.items);

        // 2. Create purchase relationships in Neo4j
        if (productIds.length > 0) {
            await session.run(
                `
                MERGE (u:User {id: $userId})
                WITH u
                UNWIND $productIds AS pId
                MERGE (p:Product {id: pId})
                MERGE (u)-[:PURCHASED]->(p)
                `,
                { userId, productIds }
            );
        }

        // 3. Clear cart in Redis
        await redisClient.del(cartKey);

        res.status(200).json({ message: 'Checkout successful' });
    } catch (err) {
        console.error('Error during checkout:', err);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await session.close();
    }
});


// 6. Get Recommendations (from Neo4j)
app.get('/api/recommendations/:productId', async (req, res) => {
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        const { productId } = req.params;
        const result = await session.run(
            `
            MATCH (p1:Product {id: $productId})<-[:PURCHASED]-(u:User)-[:PURCHASED]->(p2:Product)
            WHERE p1 <> p2
            RETURN p2.id AS id, p2.title AS title, COUNT(u) AS frequency
            ORDER BY frequency DESC
            LIMIT 3
            `,
            { productId }
        );
        const recommendations = result.records.map(record => ({
            id: record.get('id'),
            title: record.get('title')
        }));
        res.json(recommendations);
    } catch (err) {
        console.error('Error fetching recommendations from Neo4j:', err);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await session.close();
    }
});


// --- Admin Endpoints for Inspector ---
app.get('/api/admin/postgres', async (req, res) => res.json((await postgresPool.query('SELECT * FROM users')).rows));
app.get('/api/admin/mongodb', async (req, res) => res.json(await mongoClient.db('polyglot_shelf').collection('products').find({}).toArray()));
app.get('/api/admin/redis/:userId', async (req, res) => res.json(JSON.parse(await redisClient.get(`cart:${req.params.userId}`) || '{}')));
app.get('/api/admin/cassandra', async (req, res) => res.json((await cassandraClient.execute('SELECT * FROM polyglot_shelf.reviews')).rows));
app.get('/api/admin/neo4j', async (req, res) => {
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        const result = await session.run('MATCH (n) RETURN n');
        res.json(result.records.map(r => r.get('n').properties));
    } finally { await session.close(); }
});


// --- Server Startup ---
async function startServer() {
    try {
        await Promise.all([
            mongoClient.connect(),
            redisClient.connect(),
            cassandraClient.connect(),
        ]);
        console.log('✅ Connected to MongoDB, Redis, and Cassandra.');
        await neo4jDriver.verifyConnectivity();
        console.log('✅ Connected to Neo4j.');
        app.listen(PORT, () => {
            console.log(`🚀 Server is running on http://localhost:${PORT}`);
        });
    } catch (err) {
        console.error("🔥 Failed to connect to one or more databases:", err);
        process.exit(1);
    }
}

startServer();

