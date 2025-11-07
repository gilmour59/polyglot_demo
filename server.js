// server.js
const express = require('express');
const cors = require('cors');
const { mongoClient, postgresPool, redisClient, cassandraClient, neo4jDriver } = require('./db-clients');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = 3000;

// --- OLTP API Endpoints (from previous steps) ---

// 1. Get or Create User (from PostgreSQL)
app.post('/api/users/find-or-create', async (req, res) => {
    try {
        const { id, name } = req.body;
        if (!id || !name) {
            return res.status(400).json({ error: 'User ID and name are required' });
        }
        let result = await postgresPool.query('SELECT * FROM users WHERE id = $1', [id]);
        if (result.rows.length === 0) {
            await postgresPool.query('INSERT INTO users (id, name) VALUES ($1, $2)', [id, name]);
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
            imageUrl: p.imageUrl,
            series_name: p.series_name,
            series_number: p.series_number
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
        res.status(201).send();
    } catch (err) {
        console.error('Error creating review in Cassandra:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// 5. Checkout (creates relationships in Neo4j)
app.post('/api/checkout/:userId', async (req, res) => {
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        const { userId } = req.params;
        const cartJson = await redisClient.get(`cart:${userId}`);
        const cart = cartJson ? JSON.parse(cartJson) : { items: {} };
        const productIds = Object.keys(cart.items);

        if (productIds.length > 0) {
            await session.run(
                `
                UNWIND $productIds AS pId
                MERGE (u:User {id: $userId})
                MERGE (p:Product {id: pId})
                MERGE (u)-[:PURCHASED]->(p)
                `,
                { userId, productIds }
            );
            await redisClient.del(`cart:${userId}`);
        }
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
app.get('/api/admin/postgres', async (req, res) => res.json((await postgresPool.query('SELECT * FROM users ORDER BY created_at DESC')).rows));
app.get('/api/admin/mongodb', async (req, res) => res.json(await mongoClient.db('polyglot_shelf').collection('products').find({}).toArray()));
app.get('/api/admin/redis/:userId', async (req, res) => res.json(JSON.parse(await redisClient.get(`cart:${req.params.userId}`) || '{}')));
app.get('/api/admin/cassandra', async (req, res) => res.json((await cassandraClient.execute('SELECT * FROM polyglot_shelf.reviews')).rows));
app.get('/api/admin/neo4j', async (req, res) => {
    const session = neo4jDriver.session({ database: 'neo4j' });
    try {
        const result = await session.run(
            `MATCH (u:User)-[r:PURCHASED]->(p:Product) 
             RETURN u.id AS userId, p.id AS productId`
        );
        const relationships = result.records.map(record => ({
            user: record.get('userId'),
            purchased: record.get('productId')
        }));
        res.json({ relationships });
    } catch (err) {
        console.error('Error fetching Neo4j admin data:', err);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await session.close();
    }
});

// --- OLAP / Data Warehouse Endpoints ---

// This endpoint simulates an ETL (Extract, Transform, Load) process.
app.post('/api/admin/run-etl', async (req, res) => {
    console.log('Starting ETL process...');
    const pgClient = await postgresPool.connect(); // Use a single client for the transaction

    try {
        await pgClient.query('BEGIN'); // Start transaction

        // 1. CREATE WAREHOUSE TABLES (Star Schema)
        console.log('  - Ensuring data warehouse tables exist...');
        await pgClient.query(`
            CREATE TABLE IF NOT EXISTS dim_users (
                user_key SERIAL PRIMARY KEY,
                user_id VARCHAR(255) UNIQUE,
                name VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS dim_products (
                product_key SERIAL PRIMARY KEY,
                product_id VARCHAR(255) UNIQUE,
                title VARCHAR(255),
                author VARCHAR(255),
                price NUMERIC(10, 2)
            );
            CREATE TABLE IF NOT EXISTS fact_purchases (
                purchase_id SERIAL PRIMARY KEY,
                user_key INTEGER REFERENCES dim_users(user_key),
                product_key INTEGER REFERENCES dim_products(product_key),
                purchase_date DATE NOT NULL -- Removed DEFAULT
            );
        `);

        // 2. EXTRACT data from operational databases
        console.log('  - Extracting data from source databases...');
        const users = (await pgClient.query('SELECT id, name FROM users')).rows;
        const products = await mongoClient.db('polyglot_shelf').collection('products').find({}).toArray();
        const neo4jSession = neo4jDriver.session({ database: 'neo4j' });
        const purchaseRelations = (await neo4jSession.run(`MATCH (u:User)-[:PURCHASED]->(p:Product) RETURN u.id AS userId, p.id AS productId`)).records;
        await neo4jSession.close();

        // 3. TRANSFORM AND LOAD into warehouse tables
        console.log('  - Transforming and Loading data into warehouse...');
        
        // Truncate for simplicity in a demo environment
        await pgClient.query('TRUNCATE dim_users, dim_products, fact_purchases RESTART IDENTITY CASCADE');

        // Load Dimensions
        for (const user of users) {
            await pgClient.query('INSERT INTO dim_users (user_id, name) VALUES ($1, $2)', [user.id, user.name]);
        }
        for (const product of products) {
            await pgClient.query('INSERT INTO dim_products (product_id, title, author, price) VALUES ($1, $2, $3, $4)', [product._id, product.title, product.author, product.price]);
        }

        // Load Facts
        for (const record of purchaseRelations) {
            const userId = record.get('userId');
            const productId = record.get('productId');
            
            // ** THE FIX IS HERE **
            // Simulate a random purchase date within the last 7 days
            const randomDaysAgo = Math.floor(Math.random() * 7);
            const purchaseDate = new Date();
            purchaseDate.setDate(purchaseDate.getDate() - randomDaysAgo);

            await pgClient.query(`
                INSERT INTO fact_purchases (user_key, product_key, purchase_date)
                SELECT du.user_key, dp.product_key, $3
                FROM dim_users du, dim_products dp
                WHERE du.user_id = $1 AND dp.product_id = $2
            `, [userId, productId, purchaseDate]);
        }

        await pgClient.query('COMMIT'); // Commit transaction
        console.log('✅ ETL process completed successfully!');
        res.status(200).json({ message: 'ETL process completed successfully!' });
    } catch (err) {
        await pgClient.query('ROLLBACK'); // Rollback on error
        console.error('🔥 ETL process failed:', err);
        res.status(500).json({ error: 'ETL process failed' });
    } finally {
        pgClient.release();
    }
});

// --- NEW ANALYTICS ENDPOINTS ---

// Endpoint to get KPIs
app.get('/api/analytics/kpis', async (req, res) => {
    try {
        const result = await postgresPool.query(`
            SELECT
                COALESCE(SUM(dp.price), 0) AS total_revenue,
                COALESCE(COUNT(fp.purchase_id), 0) AS total_sales,
                COALESCE(COUNT(DISTINCT fp.user_key), 0) AS total_customers
            FROM fact_purchases fp
            LEFT JOIN dim_products dp ON fp.product_key = dp.product_key;
        `);
        // Note: We use LEFT JOIN and COALESCE to return 0 instead of NULL if no sales exist
        res.json(result.rows[0]);
    } catch(err) {
        console.error('Error fetching KPIs:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Endpoint to get sales by product (existing)
app.get('/api/analytics/sales-by-product', async (req, res) => {
    try {
        const result = await postgresPool.query(`
            SELECT dp.title, COUNT(fp.purchase_id) AS sales_count
            FROM fact_purchases fp
            JOIN dim_products dp ON fp.product_key = dp.product_key
            GROUP BY dp.title
            ORDER BY sales_count DESC;
        `);
        res.json(result.rows);
    } catch(err) {
        console.error('Error fetching sales by product:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Endpoint to get sales over time
app.get('/api/analytics/sales-over-time', async (req, res) => {
    try {
        const result = await postgresPool.query(`
            SELECT 
                purchase_date,
                COUNT(purchase_id) AS sales_count
            FROM fact_purchases
            GROUP BY purchase_date
            ORDER BY purchase_date ASC;
        `);
        res.json(result.rows);
    } catch(err) {
        console.error('Error fetching sales over time:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Endpoint to get top customers
app.get('/api/analytics/top-customers', async (req, res) => {
    try {
        const result = await postgresPool.query(`
            SELECT 
                du.name,
                COUNT(fp.purchase_id) AS purchase_count
            FROM fact_purchases fp
            JOIN dim_users du ON fp.user_key = du.user_key
            GROUP BY du.name
            ORDER BY purchase_count DESC
            LIMIT 5;
        `);
        res.json(result.rows);
    } catch(err) {
        console.error('Error fetching top customers:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
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