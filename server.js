// server.js
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors'); // Import the cors package
require('dotenv').config(); // Load environment variables from .env file

const app = express();
const port = process.env.PORT || 3001; // Use port from environment variable or default to 3001

// Initialize BigQuery client
const bigquery = new BigQuery({
    projectId: process.env.GOOGLE_CLOUD_PROJECT_ID, // Ensure this is set in your .env or Render config
    keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS // Path to your service account key file
});

// Middleware to parse JSON bodies
app.use(express.json());

// CORS Configuration
// Define allowed origins for your frontend application
const allowedOrigins = [
    'http://localhost:3000', // For local React development
    'http://localhost:3001', // If your React app runs on 3001 locally
    'https://scheduler-ui-roan.vercel.app', // Your Vercel frontend domain
    // Add any other specific Vercel preview URLs if needed.
    // For example, if you have branch deployments like 'https://scheduler-ui-git-branchname-your-org.vercel.app'
    // You might use a regex for more dynamic preview URLs:
    // /https:\/\/scheduler-ui-git-[a-zA-Z0-9-]+-your-org.vercel.app$/,
];

const corsOptions = {
    origin: function (origin, callback) {
        // Allow requests with no origin (like mobile apps or curl requests)
        if (!origin) return callback(null, true);
        if (allowedOrigins.indexOf(origin) === -1) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], // Allowed HTTP methods
    allowedHeaders: ['Content-Type', 'Authorization'], // Allowed headers in client request
    credentials: true, // Allow sending cookies/auth headers with cross-origin requests
};

app.use(cors(corsOptions)); // Apply CORS middleware

// Basic route for testing server status
app.get('/', (req, res) => {
    res.send('BigQuery Express Server is running!');
});

// API endpoint to fetch data from BigQuery (used by DeliveryList, Tasklist, DeliveryDetail)
app.get('/api/data', async (req, res) => {
    const { email, isAdmin, offset = 0, limit = 500, searchTerm, delCode, selectedClient } = req.query;

    console.log('Received /api/data request:');
    console.log(`Email: ${email}, isAdmin: ${isAdmin}, Offset: ${offset}, Limit: ${limit}`);
    console.log(`SearchTerm: "${searchTerm}", DelCode: "${delCode}", SelectedClient: "${selectedClient}"`);

    if (!email) {
        return res.status(400).json({ error: 'Email is required.' });
    }

    const datasetId = 'BIQuery_data';
    const tableId = 'Workflow_Details';
    const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID; // Ensure this is set

    // Base query parts
    let query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\``;
    const conditions = [];
    const params = {};

    // Condition based on user email or admin status
    // If not admin, filter by debug_emails, otherwise show all
    if (isAdmin === 'false' || isAdmin === false) { // Ensure boolean check or string 'false'
        conditions.push(`debug_emails = @email`);
        params.email = email;
    }
    // If isAdmin is 'true' or true, no email filter is applied, showing all.

    // Filter by delCode for DeliveryDetail and Tasklist
    if (delCode) {
        conditions.push(`DelCode_w_o__ = @delCode`);
        params.delCode = delCode;
        console.log(`Filtering by specific DelCode: ${delCode}`);
    } else {
        // Only apply these filters if a specific delCode is NOT provided (for DeliveryList)
        // This ensures that when delCode is provided, we get all tasks for it.
        // And when not provided (for DeliveryList), we get Step_ID = 0 and other filters.

        // Filter for specific Step_ID for DeliveryList (Step_ID = 0 for main entries)
        conditions.push(`Step_ID = 0`);
        console.log("Filtering for Step_ID = 0 (main delivery entries)");

        // Search Term (Delivery Code or Short Description) for DeliveryList
        if (searchTerm) {
            conditions.push(`(Delivery_code LIKE @searchTerm OR Short_Description LIKE @searchTerm)`);
            params.searchTerm = `%${searchTerm}%`; // Use LIKE for partial matches
            console.log(`Applying search term: ${searchTerm}`);
        }

        // Filter by Client for DeliveryList
        if (selectedClient) {
            conditions.push(`Client = @selectedClient`);
            params.selectedClient = selectedClient;
            console.log(`Filtering by client: ${selectedClient}`);
        }
    }

    // Construct WHERE clause
    if (conditions.length > 0) {
        query += ` WHERE ` + conditions.join(' AND ');
    }

    // Add LIMIT and OFFSET for pagination (only if delCode is NOT provided)
    // If delCode is provided, we want ALL tasks for that delCode, not just a paginated subset.
    if (!delCode) {
        query += ` ORDER BY Created_at DESC`; // Order by most recent for general list
        query += ` LIMIT @limit OFFSET @offset`;
        params.limit = parseInt(limit);
        params.offset = parseInt(offset);
        console.log(`Applying pagination: Limit ${limit}, Offset ${offset}`);
    } else {
        query += ` ORDER BY Step_ID ASC`; // Order tasks by step ID for a single delivery
    }

    const options = {
        query: query,
        location: 'US', // Specify your BigQuery dataset location (e.g., 'US', 'EU')
        params: params,
    };

    console.log('BigQuery SQL Query:', query);
    console.log('BigQuery Query Parameters:', params);

    try {
        const [rows] = await bigquery.query(options);
        console.log(`Fetched ${rows.length} rows from BigQuery.`);
        res.json(rows); // Send the fetched rows as JSON
    } catch (err) {
        console.error('BigQuery API error:', err);
        res.status(500).json({ error: 'Failed to fetch data from BigQuery', details: err.message });
    }
});


// API endpoint to fetch unique client names (used by DeliveryList)
app.get('/api/persons', async (req, res) => {
    const datasetId = 'BIQuery_data';
    const tableId = 'Workflow_Details';
    const projectId = process.env.GOOGLE_CLOUD_PROJECT_ID;

    const query = `
        SELECT DISTINCT Client
        FROM \`${projectId}.${datasetId}.${tableId}\`
        WHERE Client IS NOT NULL AND Client != ''
        ORDER BY Client ASC
    `;

    const options = {
        query: query,
        location: 'US',
    };

    try {
        const [rows] = await bigquery.query(options);
        const clients = rows.map(row => row.Client);
        console.log(`Fetched ${clients.length} unique clients.`);
        res.json(clients);
    } catch (err) {
        console.error('BigQuery API error fetching clients:', err);
        res.status(500).json({ error: 'Failed to fetch client data', details: err.message });
    }
});


// Start the server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
