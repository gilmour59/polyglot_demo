# **Polyglot Persistence E-commerce Demo: "The Polyglot Shelf"**

This project demonstrates the concept of Polyglot Persistence using a simple e-commerce bookstore application. It showcases how different database technologies can be chosen for specific tasks within a single application architecture to optimize for performance, scalability, and data modeling flexibility.

The application includes both an operational e-commerce frontend and a basic Business Intelligence (BI) dashboard powered by a data warehouse.

## **Architecture Overview**

The system uses a classic **Client-Server** model with a **Polyglot Persistence** layer managed by a Node.js/Express.js backend API.

* **Frontend:** A static HTML, CSS (Tailwind), and JavaScript application (ecommerce\_poly\_app.html) that interacts with the backend API. A separate dashboard.html provides BI visualizations.  
* **Backend API Server:** A Node.js/Express.js server (server.js) that acts as the intermediary, routing requests to the appropriate database based on the task.  
* **Databases:**  
  * **PostgreSQL (Relational):** Manages user accounts (OLTP) and serves as the Data Warehouse (OLAP) for analytics.  
  * **MongoDB (Document):** Stores the product catalog with its flexible schema.  
  * **Redis (Key-Value):** Handles the volatile shopping cart data with high speed.  
  * **Cassandra (Wide-Column):** Manages product reviews, optimized for high write volume.  
  * **Neo4j (Graph):** Stores purchase relationships to power the recommendation engine.

## **Prerequisites**

Before you begin, ensure you have the following installed:

* **Node.js** (v16 or later recommended) and **npm**  
* **Docker Desktop:** To run the databases easily.  
* **Databases:** You should have Docker containers running for:  
  * PostgreSQL (default port 5432\)  
  * MongoDB (default port 27017\)  
  * Redis (default port 6379\)  
  * Cassandra (default port 9042\)  
  * Neo4j (default ports 7474, 7687\)  
* **ngrok:** A tool to create a secure tunnel to your local server.  
  * brew install ngrok/ngrok/ngrok or download from [ngrok.com](https://ngrok.com/download).

## **Setup & Installation**

1. **Clone/Download:** Get the project files (backend folder \+ frontend HTML files).  
2. **Navigate to Backend:** Open your terminal in the backend project folder (where package.json is).  
3. **Configure Credentials:** **Crucially**, edit the db-clients.js file and update the user and password fields for your PostgreSQL and Neo4j connections to match your local setup.  
4. **Install Dependencies:** Run npm install to download all the required Node.js packages.  
5. **Create PostgreSQL Database:** If you haven't already, connect to psql and run CREATE DATABASE polyglot\_shelf;.  
6. **Seed Databases:** Run the seeding script once: node seed.js. This creates all tables/keyspaces and populates them with initial data.

## **Running for a Remote Presentation (with ngrok)**

This is the recommended method for sharing your live demo with remote participants.

### **Part 1: Run Your Backend API (Locally)**

1. **Start your Backend:** In your first terminal (in the backend folder), start the API server:  
   node server.js

   This serves your API at http://localhost:3000.  
2. **Start ngrok:** Open a **new, separate terminal window** and run:  
   ngrok http 3000  
