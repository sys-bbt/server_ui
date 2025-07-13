const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');
const moment = require('moment'); // Import moment for date handling

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Your main task table (e.g., componentv2)
const bigQueryTable2 = "Per_Key_Per_Day"; // Per_Key_Per_Day table
const bigQueryTable3 = "Per_Person_Per_Day";

const app = express();

// Middleware setup
// Configure CORS to allow requests from your Vercel frontend
const allowedOrigins = [
    'http://localhost:3001', // For local development
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


// Endpoint to fetch people mapping - UPDATED TO QUERY NATIVE TABLE
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
    console.log('Received request body:', JSON.stringify(req.body, null, 2)); // Log the entire request body

    // Destructure directly from req.body as the frontend sends a flat object
    const {
        Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline,
        Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp,
        Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks,
        Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at,
        Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status, sliders // 'sliders' array is present
    } = req.body;

    // The 'mainTask' object is implicitly the entire destructured body (excluding sliders for the main table update)
    const mainTask = {
        Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline,
        Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp,
        Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks,
        Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at,
        Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status
    };

    // 'perKeyPerDayRows' is directly the 'sliders' array
    const perKeyPerDayRows = sliders;

    console.log('mainTask (constructed):', mainTask); // Log mainTask
    console.log('perKeyPerDayRows (from sliders):', perKeyPerDayRows); // Log perKeyPerDayRows

    // Check if mainTask.Key is undefined before proceeding (more specific check)
    if (mainTask.Key === undefined || mainTask.Key === null) {
        console.error("mainTask.Key is missing in the request body.");
        return res.status(400).json({
            message: 'Bad Request: Task Key is missing in the request body.',
            details: 'The server expected a "Key" property for the main task but it was not found.'
        });
    }

    // Convert timestamps to BigQuery compatible format for mainTask
    const formatTimestamp = (timestamp, type) => {
        if (!timestamp) return null;
        // Remove " UTC" suffix if present before parsing
        const cleanedTimestamp = typeof timestamp === 'string' ? timestamp.replace(' UTC', '') : timestamp;
        const momentObj = moment.utc(cleanedTimestamp); // Parse as UTC
        if (type === 'TIMESTAMP') {
            return momentObj.isValid() ? momentObj.format('YYYY-MM-DD HH:mm:ss.SSSSSS') + ' UTC' : null;
        } else if (type === 'DATETIME') {
            return momentObj.isValid() ? momentObj.format('YYYY-MM-DD HH:mm:ss.SSSSSS') : null;
        }
        return null; // Default or error case
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

    // Define types for nullable parameters in mainTaskRow
    const mainTaskParameterTypes = {
        Planned_Start_Timestamp: 'TIMESTAMP',
        Planned_Delivery_Timestamp: 'TIMESTAMP',
        Created_at: 'TIMESTAMP',
        Updated_at: 'DATETIME',
        Emails: 'STRING',
        Responsibility: 'STRING',
        Client: 'STRING',
        Short_Description: 'STRING',
        Frequency___Timeline: 'STRING',
        Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
        Card_Corner_Status: 'STRING',
        // Add all other fields that can be null and are part of mainTaskRow
        // Explicitly define types for all fields that might be null
        Total_Tasks: 'INTEGER', // Assuming these are integers
        Completed_Tasks: 'INTEGER',
        Planned_Tasks: 'INTEGER',
        Percent_Tasks_Completed: 'FLOAT', // Assuming this is a float/numeric
    };

    // Define schema for Per_Key_Per_Day table inserts
    const perKeyPerDaySchema = [
        { name: 'Key', type: 'INTEGER' }, // Corrected to INTEGER
        { name: 'Day', type: 'DATE' },
        { name: 'Duration', type: 'INTEGER' }, // Corrected to INTEGER
        { name: 'Duration_Unit', type: 'STRING' },
        { name: 'Planned_Delivery_Slot', type: 'STRING', mode: 'NULLABLE' },
        { name: 'Responsibility', type: 'STRING' },
    ];

    try {
        // 1. Update the main task table (componentv2)
        // This section is commented out as per user's request to only update Per_Key_Per_Day.
        /*
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
            types: mainTaskParameterTypes, // <--- IMPORTANT: Pass the types here
            location: 'US',
        };
        const [mainTaskJob] = await bigQueryClient.createQueryJob(updateMainTaskOptions);
        await mainTaskJob.getQueryResults();
        console.log(`Main task with Key ${mainTask.Key} updated successfully.`);
        */
        // End of commented out section for main task table update

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
        const [deleteJob] = await bigQueryClient.createQueryJob(deletePerKeyOptions);
        await deleteJob.getQueryResults();
        console.log(`Existing Per_Key_Per_Day entries for Key ${mainTask.Key} deleted.`);

        // 3. Insert new Per_Key_Per_Day entries
        if (perKeyPerDayRows && perKeyPerDayRows.length > 0) {
            const insertRows = perKeyPerDayRows.map(row => ({
                Key: parseInt(row.Key, 10), // Convert Key to INTEGER for insertion
                Day: row.Day,
                Duration: row.Duration, // This is now in minutes from frontend
                Duration_Unit: row.Duration_Unit, // This is now 'Minutes' from frontend
                Planned_Delivery_Slot: row.Planned_Delivery_Slot,
                Responsibility: row.Responsibility,
            }));

            await bigQueryClient
                .dataset(bigQueryDataset)
                .table(bigQueryTable2)
                .insert(insertRows, { schema: perKeyPerDaySchema });
            console.log(`New Per_Key_Per_Day entries for Key ${mainTask.Key} inserted successfully.`);
        }

        res.status(200).send({ message: 'Task and associated schedule data updated successfully.' });

    } catch (error) {
        console.error('Error updating task and schedule in BigQuery:', error);
        if (error.response && error.response.insertErrors) {
            console.error('BigQuery specific insert errors details:');
            error.response.insertErrors.forEach((insertError, index) => {
                console.error(`  Row ${index} had errors:`);
                insertError.errors.forEach(e => console.error(`    - Reason: ${e.reason}, Message: ${e.message}`));
                console.error('  Raw row that failed:', JSON.stringify(insertError.row, null, 2));
            });
        } else if (error.code && error.errors) {
            console.error('Google Cloud API Error:', JSON.stringify(error.errors, null, 2));
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
    console.log("hi", req.params)
    const query = `
        DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        WHERE DelCode_w_o__ = @deliveryCode
    `;

    const options = {
        query: query,
        params: { deliveryCode },
        types: { deliveryCode: 'STRING' }, // Explicitly define type for deliveryCode
    };

    try {
        const [job] = await bigQueryClient.createQueryJob(options);
        await job.getQueryResults();
        res.status(200).send({ message: 'All tasks with the specified delivery code were deleted successfully.' });
    } catch (error) {
        console.error('Error deleting tasks from BigQuery:', error);
        res.status(500).send({ error: 'Failed to delete tasks from BigQuery.' });
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
