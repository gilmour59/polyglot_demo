// server.js
const express = require('express');
const cors = require('cors');
const { mongoClient, postgresPool, redisClient, cassandraClient, neo4jDriver } = require('./db-clients');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = 3000;

// --- API Endpoints ---

// 1. Get User (from PostgreSQL)
app.get('/api/users/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const result = await postgresPool.query('SELECT * FROM users WHERE id = $1', [id]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        res.json(result.rows[0]);
    } catch (err) {
        console.error('Error fetching user from PostgreSQL:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 2. Get Products (from MongoDB)
app.get('/api/products', async (req, res) => {
    try {
        const db = mongoClient.db('polyglot_shelf');
        const productsFromDb = await db.collection('products').find({}).toArray();

        // ** THE FIX IS HERE **
        // Map the MongoDB '_id' field to a standard 'id' field for the frontend.
        const products = productsFromDb.map(p => {
            return {
                id: p._id, // Create the 'id' property
                title: p.title,
                author: p.author,
                price: p.price,
                imageUrl: p.imageUrl
            };
        });

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

// 4. Get Reviews (from Cassandra)
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

// 5. Get Recommendations (from Neo4j)
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


// --- Server Startup ---
async function startServer() {
    try {
        // Connect to all databases on startup
        await Promise.all([
            mongoClient.connect(),
            redisClient.connect(),
            cassandraClient.connect(),
            // postgresPool and neo4jDriver connect on first query
        ]);
        console.log('✅ Connected to MongoDB, Redis, and Cassandra.');

        // Verify connection to Neo4j
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