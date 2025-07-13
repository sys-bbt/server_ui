const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');
const moment = require('moment'); // Import moment for date handling

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Your main task table
const bigQueryTable2 = "Per_Key_Per_Day";
const bigQueryTable3 = "Per_Person_Per_Day";

const app = express();

// Middleware setup
// Configure CORS to allow requests from your Vercel frontend
const allowedOrigins = [
    'http://localhost:3000', // For local development
    /^https:\/\/.*\.vercel\.app$/, // Regex to match any subdomain of vercel.app with HTTPS
    'https://scheduler-ui-roan.vercel.app' // Explicitly keep your main Vercel URL
];

app.use(cors({
    origin: function (origin, callback) {
        // Allow requests with no origin (like mobile apps or curl requests)
        if (!origin) return callback(null, true);

        // Check if the origin is in the allowedOrigins array or matches a regex
        const isAllowed = allowedOrigins.some(allowedOrigin => {
            if (typeof allowedOrigin === 'string') {
                return allowedOrigin === origin;
            } else if (allowedOrigin instanceof RegExp) {
                return allowedOrigin.test(origin);
            }
            return false;
        });

        if (!isAllowed) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], // Explicitly include OPTIONS
    allowedHeaders: ['Content-Type', 'Authorization'], // Allow necessary headers
    credentials: true // Allow cookies to be sent
}));
app.use(express.json()); // This middleware parses JSON request bodies

console.log('DEBUG: GOOGLE_PROJECT_ID:', process.env.GOOGLE_PROJECT_ID);
console.log('DEBUG: BIGQUERY_CLIENT_EMAIL:', process.env.BIGQUERY_CLIENT_EMAIL);
console.log('DEBUG: BIGQUERY_PRIVATE_KEY exists:', !!process.env.BIGQUERY_PRIVATE_KEY);
if (process.env.BIGQUERY_PRIVATE_KEY) {
    console.log('DEBUG: First 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.substring(0, 50));
    console.log('DEBUG: Last 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.slice(-50));
    console.log('DEBUG: Private key contains \\n:', process.env.BIGQUERY_PRIVATE_KEY.includes('\\n'));
}

const bigQueryClient = new BigQuery({
    projectId: projectId,
    credentials: {
        client_email: process.env.BIGQUERY_CLIENT_EMAIL,
        private_key: process.env.BIGQUERY_PRIVATE_KEY ? process.env.BIGQUERY_PRIVATE_KEY.replace(/\\n/g, '\n') : undefined,
    },
});

// Define admin emails on the backend for consistency and security
const ADMIN_EMAILS_BACKEND = [
    "neelam.p@brightbraintech.com",
    "meghna.j@brightbraintech.com",
    "zoya.a@brightbraintech.com",
    "shweta.g@brightbraintech.com",
    "hitesh.r@brightbraintech.com"
];

// Define the special "System" email for tasks that should be globally visible to non-admins
const SYSTEM_EMAIL_FOR_GLOBAL_TASKS = "systems@brightbraintech.com";


// Endpoint to fetch people mapping
app.get('/api/people-mapping', async (req, res) => {
    // IMPORTANT: Replace 'People_To_Email_Mapping_Native' with the exact name
    // you used when creating the native BigQuery table from your Google Sheet.
    const NATIVE_PEOPLE_TABLE = 'People_To_Email_Mapping_Native'; // <--- CHANGE THIS TO YOUR NEW NATIVE TABLE NAME
    const query = `
        SELECT Current_Employes, Emp_Emails
        FROM \`${projectId}.${bigQueryDataset}.${NATIVE_PEOPLE_TABLE}\`
    `;

    try {
        const [rows] = await bigQueryClient.query(query);
        const formattedRows = rows.map(row => ({
            Current_Employes: row.Current_Employes,
            Emp_Emails: row.Emp_Emails
        }));
        res.status(200).json(formattedRows);
    } catch (error) {
        console.error('Error fetching people mapping from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch people mapping data.' });
    }
});

// Modified /api/data route (GET workflow headers only, with filtering for non-admins)
app.get('/api/data', async (req, res) => {
    const userEmail = req.query.email; // Get email from query parameter
    const searchQuery = req.query.searchQuery; // Get search query from parameter
    const clientFilter = req.query.clientFilter; // Get client filter from parameter

    let query;
    let params = {};
    let whereClauses = [`Step_ID = 0`]; // Always filter for workflow headers

    // Add user-specific filtering for non-admins
    if (userEmail && !ADMIN_EMAILS_BACKEND.includes(userEmail)) {
        whereClauses.push(`DelCode_w_o__ IN (
            SELECT DISTINCT DelCode_w_o__
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            WHERE Emails LIKE @userEmail OR Emails LIKE @systemEmail
        )`);
        params.userEmail = `%${userEmail}%`;
        params.systemEmail = `%${SYSTEM_EMAIL_FOR_GLOBAL_TASKS}%`;
        console.log(`Filtering workflow headers for non-admin user: ${userEmail}`);
    } else if (userEmail && ADMIN_EMAILS_BACKEND.includes(userEmail)) {
        console.log(`Fetching all workflow headers for admin user: ${userEmail}`);
    } else {
        console.log(`Fetching all workflow headers (no user email provided or default behavior)`);
    }

    // Add search query filtering
    if (searchQuery) {
        // Search in Task_Details or Delivery_code
        whereClauses.push(`(Task_Details LIKE @searchQuery OR Delivery_code LIKE @searchQuery)`);
        params.searchQuery = `%${searchQuery}%`;
        console.log(`Applying search filter: ${searchQuery}`);
    }

    // Add client filter
    if (clientFilter) {
        whereClauses.push(`Client = @clientFilter`);
        params.clientFilter = clientFilter;
        console.log(`Applying client filter: ${clientFilter}`);
    }

    query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
             WHERE ${whereClauses.join(' AND ')}`;

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            location: 'US', // Specify your BigQuery dataset location
        });
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching data from BigQuery for /api/data:', error);
        res.status(500).send({ error: 'Failed to fetch data from BigQuery.' });
    }
});

// NEW ENDPOINT: /api/workflow-details/:deliveryCode (GET all tasks for a specific workflow)
app.get('/api/workflow-details/:deliveryCode', async (req, res) => {
    const { deliveryCode } = req.params;
    const query = `
        SELECT *
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        WHERE DelCode_w_o__ = @deliveryCode
    `;
    const params = { deliveryCode: deliveryCode };

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            location: 'US',
        });
        res.status(200).json(rows);
    } catch (error) {
        console.error(`Error fetching workflow details for ${deliveryCode} from BigQuery:`, error);
        res.status(500).send({ error: `Failed to fetch workflow details for ${deliveryCode}.` });
    }
});


// NEW ENDPOINT: /api/per-key-per-day-by-key
app.get('/api/per-key-per-day-by-key', async (req, res) => {
    const { key } = req.query; // Get the key from query parameters
    if (!key) {
        return res.status(400).send({ error: 'Key parameter is required.' });
    }

    const query = `
        SELECT Key, Day, Duration, Duration_Unit, Planned_Delivery_Slot, Responsibility
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`
        WHERE Key = @key
    `;
    const params = { key: parseInt(key, 10) }; // Convert key to INT64 for comparison
    const queryTypes = {
        key: 'INT64' // Explicitly define key as INT64 based on schema
    };

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            types: queryTypes, // Pass types here
            location: 'US', // Specify your BigQuery dataset location
        });

        const groupedData = {
            totalDuration: 0,
            entries: []
        };
        rows.forEach(row => {
            groupedData.entries.push(row);
            groupedData.totalDuration += row.Duration || 0; // Assuming Duration is the hours
        });

        if (rows.length === 0) {
            return res.status(404).send({ message: 'No entries found for this key.' });
        }

        res.status(200).json(groupedData);
    } catch (error) {
        console.error(`Error fetching Per_Key_Per_Day data for Key ${key} from BigQuery:`, error);
        res.status(500).send({ error: `Failed to fetch Per_Key_Per_Day data for Key ${key}.` });
    }
});


// Existing /api/per-key-per-day route (kept for other potential uses)
app.get('/api/per-key-per-day', async (req, res) => {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
    try {
        const [rows] = await bigQueryClient.query(query);
        const groupedData = {};
        rows.forEach(row => {
            const key = row.Key;
            if (!groupedData[key]) {
                groupedData[key] = {
                    totalDuration: 0,
                    entries: []
                };
            }
            groupedData[key].entries.push(row);
            // Assuming Duration_In_Minutes is always present and a number
            groupedData[key].totalDuration += row.Duration_In_Minutes || 0;
        });
        res.status(200).json(groupedData);
    } catch (error) {
        console.error('Error fetching per-key-per-day data from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch per-key-per-day data from BigQuery.' });
    }
});

// Existing /api/per-person-per-day route
app.get('/api/per-person-per-day', async (req, res) => {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\``;
    try {
        const [rows] = await bigQueryClient.query(query);
        res.status(200).json(rows);
    }
    catch (error) {
        console.error('Error fetching per-person-per-day data from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch per-person-per-day data from BigQuery.' });
    }
});

// Modified POST route to handle both main task and Per_Key_Per_Day updates
app.post('/api/post', async (req, res) => {
    console.log('Backend: Received POST request to /api/post');
    console.log('Backend: Request body:', JSON.stringify(req.body, null, 2)); // Log the entire request body

    // Correctly destructure mainTask and perKeyPerDayRows from req.body
    const { mainTask, perKeyPerDayRows } = req.body;

    console.log('Backend: mainTask (destructured):', JSON.stringify(mainTask, null, 2));
    console.log('Backend: perKeyPerDayRows (destructured):', JSON.stringify(perKeyPerDayRows, null, 2));

    // Check if mainTask or its Key is missing
    if (!mainTask || mainTask.Key === undefined || mainTask.Key === null) {
        console.error("Backend: mainTask or mainTask.Key is missing in the request body.");
        return res.status(400).json({
            message: 'Bad Request: Task data or Task Key is missing in the request body.',
            details: 'The server expected a "mainTask" object with a "Key" property but it was not found or was incomplete.'
        });
    }

    // Convert timestamps to BigQuery compatible format for mainTask
    const formatTimestamp = (timestamp, type) => {
        if (!timestamp) return null;
        // Remove " UTC" suffix if present before parsing, then parse as UTC
        const cleanedTimestamp = typeof timestamp === 'string' ? timestamp.replace(' UTC', '') : timestamp;
        const momentObj = moment.utc(cleanedTimestamp);
        if (type === 'TIMESTAMP') {
            return momentObj.isValid() ? momentObj.format('YYYY-MM-DD HH:mm:ss.SSSSSS') + ' UTC' : null;
        } else if (type === 'DATETIME') {
            return momentObj.isValid() ? momentObj.format('YYYY-MM-DD HH:mm:ss.SSSSSS') : null;
        }
        return null;
    };

    const formattedPlannedStartTimestamp = formatTimestamp(mainTask.Planned_Start_Timestamp, 'TIMESTAMP');
    const formattedPlannedDeliveryTimestamp = formatTimestamp(mainTask.Planned_Delivery_Timestamp, 'TIMESTAMP');
    const formattedCreatedAt = formatTimestamp(mainTask.Created_at, 'TIMESTAMP');
    const formattedUpdatedAt = formatTimestamp(mainTask.Updated_at, 'DATETIME');


    // Prepare data for the main task table update
    const mainTaskRow = {
        Key: mainTask.Key,
        Delivery_code: mainTask.Delivery_code,
        DelCode_w_o__: mainTask.DelCode_w_o__,
        Step_ID: mainTask.Step_ID,
        Task_Details: mainTask.Task_Details,
        Frequency___Timeline: mainTask.Frequency___Timeline,
        Client: mainTask.Client,
        Short_Description: mainTask.Short_Description,
        Planned_Start_Timestamp: formattedPlannedStartTimestamp,
        Planned_Delivery_Timestamp: formattedPlannedDeliveryTimestamp,
        Responsibility: mainTask.Responsibility,
        Current_Status: mainTask.Current_Status,
        // Removed 'Email' here as it's not a column in BigQuery table
        Emails: mainTask.Emails, 
        Total_Tasks: mainTask.Total_Tasks,
        Completed_Tasks: mainTask.Completed_Tasks,
        Planned_Tasks: mainTask.Planned_Tasks,
        Percent_Tasks_Completed: mainTask.Percent_Tasks_Completed,
        Created_at: formattedCreatedAt,
        Updated_at: formattedUpdatedAt,
        Time_Left_For_Next_Task_dd_hh_mm_ss: mainTask.Time_Left_For_Next_Task_dd_hh_mm_ss,
        Card_Corner_Status: mainTask.Card_Corner_Status,
    };

    // Define types for nullable parameters in mainTaskRow for BigQuery UPDATE
    const mainTaskParameterTypes = {
        Key: 'INTEGER', // Assuming Key is an INTEGER in BigQuery
        Delivery_code: 'STRING',
        DelCode_w_o__: 'STRING',
        Step_ID: 'INTEGER',
        Task_Details: 'STRING',
        Frequency___Timeline: 'STRING',
        Client: 'STRING',
        Short_Description: 'STRING',
        Planned_Start_Timestamp: 'TIMESTAMP',
        Planned_Delivery_Timestamp: 'TIMESTAMP',
        Responsibility: 'STRING',
        Current_Status: 'STRING',
        // Removed 'Email' type definition
        Emails: 'STRING',
        Total_Tasks: 'INTEGER',
        Completed_Tasks: 'INTEGER',
        Planned_Tasks: 'INTEGER',
        Percent_Tasks_Completed: 'FLOAT',
        Created_at: 'TIMESTAMP',
        Updated_at: 'DATETIME',
        Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
        Card_Corner_Status: 'STRING',
    };

    // Define schema for Per_Key_Per_Day table inserts
    const perKeyPerDaySchema = [
        { name: 'Key', type: 'INTEGER' },
        { name: 'Day', type: 'DATE' },
        { name: 'Duration', type: 'INTEGER' },
        { name: 'Duration_Unit', type: 'STRING' },
        { name: 'Planned_Delivery_Slot', type: 'STRING', mode: 'NULLABLE' },
        { name: 'Responsibility', type: 'STRING' },
    ];

    try {
        // 1. Update the main task table (componentv2)
        const updateMainTaskQuery = `
            UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            SET
                Delivery_code = @Delivery_code,
                DelCode_w_o__ = @DelCode_w_o__,
                Step_ID = @Step_ID,
                Task_Details = @Task_Details,
                Frequency___Timeline = @Frequency___Timeline,
                Client = @Client,
                Short_Description = @Short_Description,
                Planned_Start_Timestamp = @Planned_Start_Timestamp,
                Planned_Delivery_Timestamp = @Planned_Delivery_Timestamp,
                Responsibility = @Responsibility,
                Current_Status = @Current_Status,
                Emails = @Emails,
                Total_Tasks = @Total_Tasks,
                Completed_Tasks = @Completed_Tasks,
                Planned_Tasks = @Planned_Tasks,
                Percent_Tasks_Completed = @Percent_Tasks_Completed,
                Created_at = @Created_at,
                Updated_at = @Updated_at,
                Time_Left_For_Next_Task_dd_hh_mm_ss = @Time_Left_For_Next_Task_dd_hh_mm_ss,
                Card_Corner_Status = @Card_Corner_Status
            WHERE Key = @Key
        `;
        const updateMainTaskOptions = {
            query: updateMainTaskQuery,
            params: mainTaskRow,
            types: mainTaskParameterTypes, // Pass the types here
            location: 'US',
        };
        console.log('Backend: Executing main task update query with params:', JSON.stringify(mainTaskRow, null, 2));
        const [mainTaskJob] = await bigQueryClient.createQueryJob(updateMainTaskOptions);
        await mainTaskJob.getQueryResults();
        console.log(`Backend: Main task with Key ${mainTask.Key} updated successfully.`);


        // 2. Delete existing Per_Key_Per_Day entries for this Key
        const deletePerKeyQuery = `
            DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`
            WHERE Key = @Key
        `;
        const deletePerKeyOptions = {
            query: deletePerKeyQuery,
            params: { Key: parseInt(mainTask.Key, 10) }, // Convert Key to INT64 for deletion
            types: { Key: 'INT64' }, // Explicitly define type for Key as INT64
            location: 'US',
        };
        console.log('Backend: Deleting existing perKeyPerDayRows for Key:', mainTask.Key);
        const [deleteJob] = await bigQueryClient.createQueryJob(deletePerKeyOptions);
        await deleteJob.getQueryResults();
        console.log(`Backend: Existing Per_Key_Per_Day entries for Key ${mainTask.Key} deleted.`);


        // 3. Insert new Per_Key_Per_Day entries
        if (perKeyPerDayRows && perKeyPerDayRows.length > 0) {
            const insertRows = perKeyPerDayRows.map(row => ({
                Key: parseInt(mainTask.Key, 10), // Use mainTask.Key for the Key column
                Day: row.Day, // Use row.Day (capitalized)
                Duration: parseInt(row.Duration, 10), // Use row.Duration (capitalized)
                Duration_Unit: row.Duration_Unit, // Use row.Duration_Unit (capitalized)
                Planned_Delivery_Slot: row.Planned_Delivery_Slot || null, // Use row.Planned_Delivery_Slot (capitalized)
                Responsibility: row.Responsibility, // Use row.Responsibility (capitalized)
            }));

            console.log('Backend: Logging data types for Per_Key_Per_Day rows before insertion:');
            insertRows.forEach((row, index) => {
                console.log(`Backend: Row ${index}:`);
                for (const key in row) {
                    if (Object.hasOwnProperty.call(row, key)) {
                        console.log(`Backend:   ${key}: Value = ${row[key]}, Type = ${typeof row[key]}`);
                    }
                }
            });

            await bigQueryClient
                .dataset(bigQueryDataset)
                .table(bigQueryTable2)
                .insert(insertRows, { schema: perKeyPerDaySchema });
            console.log(`Backend: New Per_Key_Per_Day entries for Key ${mainTask.Key} inserted successfully.`);
        } else {
            console.log('Backend: No perKeyPerDayRows to insert.');
        }

        res.status(200).send({ message: 'Task and associated schedule data updated successfully.' });

    } catch (error) {
        console.error('Backend: Error updating task and schedule in BigQuery:', error);
        if (error.response && error.response.insertErrors) {
            console.error('Backend: BigQuery specific insert errors details:');
            error.response.insertErrors.forEach((insertError, index) => {
                console.error(`Backend:   Row ${index} had errors:`);
                insertError.errors.forEach(e => console.error(`Backend:     - Reason: ${e.reason}, Message: ${e.message}`));
                console.error('Backend:   Raw row that failed:', JSON.stringify(insertError.row, null, 2));
            });
        } else if (error.code && error.errors) {
            console.error('Backend: Google Cloud API Error:', JSON.stringify(error.errors, null, 2));
        }

        res.status(500).json({
            message: 'Failed to update task due to a backend error.',
            details: error.message || 'Unknown server error.',
            bigQueryErrorDetails: error.response?.insertErrors ? JSON.stringify(error.response.insertErrors) : null,
        });
    }
});

// Delete Task from BigQuery
app.delete('/api/data/:deliveryCode', async (req, res) => {
    const { deliveryCode } = req.params;
    console.log("Backend: Delete request for deliveryCode:", deliveryCode);
    const query = `
        DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        WHERE DelCode_w_o__ = @deliveryCode
    `;

    const options = {
        query: query,
        params: { deliveryCode },
    };

    try {
        const [job] = await bigQueryClient.createQueryJob(options);
        await job.getQueryResults();
        res.status(200).send({ message: 'All tasks with the specified delivery code were deleted successfully.' });
    } catch (error) {
        console.error('Backend: Error deleting tasks from BigQuery:', error);
        res.status(500).send({ error: 'Failed to delete tasks from BigQuery.' });
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
