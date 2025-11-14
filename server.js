const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');
const moment = require('moment'); 

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Your main task table
const bigQueryTable2 = "Per_Key_Per_Day";
const bigQueryTable3 = "Per_Person_Per_Day";

const app = express();

// Middleware setup
const allowedOrigins = [
    'http://localhost:3000', 
    /^https:\/\/.*\.vercel\.app$/, 
    'https://scheduler-ui-roan.vercel.app' 
];

// --- 1. UPDATED CORS CONFIGURATION ---
app.use(cors({
    origin: function (origin, callback) {
        if (!origin) return callback(null, true);

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
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], 
    allowedHeaders: ['Content-Type', 'Authorization'], 
    credentials: true 
}));
app.use(express.json()); 

console.log('DEBUG: GOOGLE_PROJECT_ID:', process.env.GOOGLE_PROJECT_ID);
console.log('DEBUG: BIGQUERY_CLIENT_EMAIL:', process.env.BIGQUERY_CLIENT_EMAIL);
console.log('DEBUG: BIGQUERY_PRIVATE_KEY exists:', !!process.env.BIGQUERY_PRIVATE_KEY);

const bigQueryClient = new BigQuery({
    projectId: projectId,
    credentials: {
        client_email: process.env.BIGQUERY_CLIENT_EMAIL,
        private_key: process.env.BIGQUERY_PRIVATE_KEY ? process.env.BIGQUERY_PRIVATE_KEY.replace(/\\n/g, '\n') : undefined,
    },
});

// Define admin emails on the backend for consistency and security
const ADMIN_EMAILS_BACKEND = [
    "systems@brightbraintech.com",
    "neelam.p@brightbraintech.com",
    "meghna.j@brightbraintech.com",
    "divya.s@brightbraintech.com",
    "zoya.a@brightbraintech.com",
    "altaf.s@brightbraintech.com",
    "arvanbir.s@brightbraintech.com"
];

const SYSTEM_EMAIL_FOR_GLOBAL_TASKS = "systems@brightbraintech.com";


// Endpoint to fetch people mapping
app.get('/api/people-mapping', async (req, res) => {
    const NATIVE_PEOPLE_TABLE = 'People_To_Email_Mapping_Native'; // <--- CHANGE THIS TO YOUR NEW NATIVE TABLE NAME IF DIFFERENT
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

// 🚀 NEW ENDPOINT: Fetch only Active Unique Clients for the Filter Dropdown 🚀
app.get('/api/active-clients', async (req, res) => {
    // This query assumes your main table (bigQueryTable) has the 'Client' and 'Inactive' columns.
    // We are filtering for rows where 'Inactive' is explicitly 'Active'.
    const query = `
        SELECT DISTINCT Client
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        WHERE Inactive = 'Active' 
        AND Client IS NOT NULL 
        ORDER BY Client
    `;

    try {
        const [rows] = await bigQueryClient.query(query);
        // Map the result to an array of client names
        const activeClients = rows.map(row => row.Client);
        console.log(`Backend: Fetched ${activeClients.length} active unique clients.`);
        res.status(200).json(activeClients);
    } catch (error) {
        console.error('Error fetching active clients from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch active client list.' });
    }
});


// GET workflow headers only, with filtering for non-admins
app.get('/api/data', async (req, res) => {
    const userEmail = req.query.email; 
    const searchQuery = req.query.searchQuery; 
    const clientFilter = req.query.clientFilter; 

    let query;
    let params = {};
    let whereClauses = [`Step_ID = 0`]; 

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

    if (searchQuery) {
        whereClauses.push(`(Task_Details LIKE @searchQuery OR Delivery_code LIKE @searchQuery)`);
        params.searchQuery = `%${searchQuery}%`;
        console.log(`Applying search filter: ${searchQuery}`);
    }

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
            location: 'US', 
        });
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching data from BigQuery for /api/data:', error);
        res.status(500).send({ error: 'Failed to fetch data from BigQuery.' });
    }
});

// FIXED ENDPOINT: /api/workflow-details/:deliveryCode (GET all tasks for a specific workflow)
app.get('/api/workflow-details/:deliveryCode', async (req, res) => {
    const { deliveryCode } = req.params;
    const query = `
        SELECT 
            Key, 
            Delivery_code,
            DelCode_w_o__,
            Step_ID,
            Task_Details,
            Frequency___Timeline,
            Client,
            Short_Description,
            Planned_Start_Timestamp,
            Planned_Delivery_Timestamp,
            Responsibility,
            Current_Status,
            Emails,
            Total_Tasks,
            Completed_Tasks,
            Planned_Tasks,
            Percent_Tasks_Completed,
            Created_at,
            Updated_at,
            Time_Left_For_Next_Task_dd_hh_mm_ss,
            Card_Corner_Status
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
    const { key } = req.query; 
    if (!key) {
        return res.status(400).send({ error: 'Key parameter is required.' });
    }

    const query = `
        SELECT Key, Day, Duration, Duration_Unit, Planned_Delivery_Slot, Responsibility
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`
        WHERE Key = @key
    `;
    const params = { key: parseInt(key, 10) }; 
    const queryTypes = {
        key: 'INT64' 
    };

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            types: queryTypes, 
            location: 'US', 
        });

        const groupedData = {
            totalDuration: 0,
            entries: []
        };
        rows.forEach(row => {
            groupedData.entries.push(row);
            groupedData.totalDuration += row.Duration || 0; 
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
    

    const { mainTask, perKeyPerDayRows } = req.body;

        // Check if mainTask or its Key is missing
    if (!mainTask || mainTask.Key === undefined || mainTask.Key === null || String(mainTask.Key) === '') {
        console.error("Backend: mainTask or mainTask.Key is missing or empty in the request body.");
        return res.status(400).json({
            message: 'Bad Request: Task data or Task Key is missing in the request body.',
            details: 'The server expected a "mainTask" object with a non-empty "Key" property but it was not found or was incomplete.'
        });
    }

    const taskKeyString = String(mainTask.Key);
    const userEmail = mainTask.Email; // Expecting user email from frontend payload

    // --- 2. SERVER-SIDE RESPONSIBILITY CHANGE VALIDATION (SECURITY CHECK) ---
    try {
        const fetchCurrentTaskQuery = `
            SELECT Responsibility
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            WHERE Key = @key
        `;
        const fetchOptions = {
            query: fetchCurrentTaskQuery,
            params: { key: parseInt(taskKeyString, 10) },
            types: { key: 'INT64' },
            location: 'US',
        };

        const [currentRows] = await bigQueryClient.query(fetchOptions);
        const currentTask = currentRows[0];
        
        // Check if the task exists and the Responsibility field is actually changing
        if (currentTask && currentTask.Responsibility !== mainTask.Responsibility) {
            
            const isAdmin = ADMIN_EMAILS_BACKEND.includes(userEmail);
            
            // If the user is NOT an admin, reject the change
            if (!isAdmin) {
                console.warn(`SECURITY ALERT: Non-admin user ${userEmail} attempted to change Responsibility for Key ${taskKeyString} from "${currentTask.Responsibility}" to "${mainTask.Responsibility}".`);
                return res.status(403).json({
                    message: 'Forbidden: You do not have permission to change the Responsibility for an existing task.',
                    details: 'Only Admin users can reassign tasks.'
                });
            }
        }
    } catch (fetchError) {
        console.error("Backend: Error during security check (fetching current task state):", fetchError);
        // Do NOT block the request on this error to avoid an accidental DoS if BigQuery is slow, 
        // but rely on the main update being accurate. A more rigorous implementation might halt here.
    }
    // --- END SERVER-SIDE VALIDATION ---


    // Convert timestamps to BigQuery compatible format for mainTask
    const formatTimestamp = (timestamp, type) => {
        if (!timestamp) return null;
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
            types: mainTaskParameterTypes, 
            location: 'US',
        };
        console.log('Backend: Executing main task update query...');
        const [mainTaskJob] = await bigQueryClient.createQueryJob(updateMainTaskOptions);
        await mainTaskJob.getQueryResults();
        console.log(`Backend: Main task with Key ${mainTask.Key} updated successfully.`);


        // 2. Safely Update/Replace Per_Key_Per_Day using MERGE
        if (perKeyPerDayRows && perKeyPerDayRows.length > 0) {
            
            // The source data for MERGE must be structured. Since you only have one row, 
            // we will create a one-row temporary table using UNNEST.

            const newRow = perKeyPerDayRows[0];
            const targetKey = parseInt(mainTask.Key, 10);
            
            // A more straightforward and less error-prone way is two sequential MERGE/DML statements:
            // 2a. DELETE all existing rows for this key. (This is the failure point, but MERGE can fix it)
            
            const deleteMergeQuery = `
                MERGE INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` AS T
                USING (
                    SELECT @targetKey AS Key
                ) AS S
                ON T.Key = S.Key
                WHEN MATCHED THEN DELETE
            `;
            
            const deleteMergeOptions = {
                query: deleteMergeQuery,
                params: { targetKey: targetKey },
                types: { targetKey: 'INT64' },
                location: 'US',
            };
            
            console.log('Backend: Deleting existing perKeyPerDayRows using MERGE...');
            const [deleteMergeJob] = await bigQueryClient.createQueryJob(deleteMergeOptions);
            await deleteMergeJob.getQueryResults(); // Wait for delete to complete
            console.log(`Backend: Existing Per_Key_Per_Day entries for Key ${targetKey} deleted using MERGE.`);


            // 2b. Insert new Per_Key_Per_Day entries (Same as previous step 3)
            const insertRows = perKeyPerDayRows.map(row => ({
                Key: targetKey, // Use the fixed integer key
                Day: row.Day, 
                Duration: parseInt(row.Duration, 10), 
                Duration_Unit: row.Duration_Unit, 
                Planned_Delivery_Slot: row.Planned_Delivery_Slot || null, 
                Responsibility: row.Responsibility, 
            }));
            
            const perKeyPerDaySchema = [
                { name: 'Key', type: 'INTEGER' },
                { name: 'Day', type: 'DATE' },
                { name: 'Duration', type: 'INTEGER' },
                { name: 'Duration_Unit', type: 'STRING' },
                { name: 'Planned_Delivery_Slot', type: 'STRING', mode: 'NULLABLE' },
                { name: 'Responsibility', type: 'STRING' },
            ];

            await bigQueryClient
                .dataset(bigQueryDataset)
                .table(bigQueryTable2)
                .insert(insertRows, { schema: perKeyPerDaySchema });
            console.log(`Backend: New Per_Key_Per_Day entries for Key ${targetKey} inserted successfully.`);
            
        } else {
            console.log('Backend: No perKeyPerDayRows to insert.');
        }


        res.status(200).send({ message: 'Task and associated schedule data updated successfully.' });

    } catch (error) {
        console.error('Backend: Error updating task and schedule in BigQuery:', error);
        if (error.response && error.response.insertErrors) {
            console.error('Backend: BigQuery specific insert errors details:');
            error.response.insertErrors.forEach((insertError, index) => {
                console.error(`Backend: Row ${index} had errors:`);
                insertError.errors.forEach(e => console.error(`Backend: - Reason: ${e.reason}, Message: ${e.message}`));
                console.error('Backend: Raw row that failed:', JSON.stringify(insertError.row, null, 2));
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

