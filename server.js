const express = require('express');
const cors = require('cors'); // Ensure cors is imported
const app = express();

const { BigQuery } = require('@google-cloud/bigquery');
const { google } = require('googleapis');
const path = require('path');
const dotenv = require('dotenv');

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Main table for deliveries and tasks
const bigQueryTable2 = "Per_Key_Per_Day"; // Used for /api/per-key-per-day (durations)
const bigQueryTable3 = "Per_Person_Per_Day"; // Used for /api/per-person-per-day (person data)
const personEmailMappingSheetId = '1CeW4uPYUSn3EOSbopRK8hqWORpLcwmcy2Zo7RzuiECo'; // Placeholder, add your actual Sheet ID
const personEmailMappingSheetRange = 'People To Email Mapping!A1:B'; // Placeholder, add your actual Sheet range


// --- CRITICAL CORS Configuration ---
// This must be placed BEFORE any app.use(express.json()) or app.get/post/put routes
const allowedOrigins = [
    'https://scheduler-ui-roan.vercel.app', // Your Vercel frontend URL
    'http://localhost:3000', // For testing on your computer (React default)
    'http://localhost:5173', // For testing on your computer (Vite default, if you use it)
    'http://localhost:3001' // If your frontend talks to backend on 3001 locally
];

app.use(cors({
    origin: function (origin, callback) {
        // Allow requests with no origin (e.g., from Postman, curl, or same-origin direct file access)
        if (!origin) {
            return callback(null, true);
        }
        // Check if the requesting origin is in our allowed list
        if (allowedOrigins.indexOf(origin) === -1) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            console.warn(`CORS Blocked: Origin ${origin} not in allowed list.`);
            return callback(new Error(msg), false);
        }
        console.log(`CORS Allowed: Origin ${origin}`);
        return callback(null, true);
    },
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', // Ensure all methods your API uses are listed
    credentials: true, // Keep this if your frontend sends cookies or auth tokens
    allowedHeaders: ['Content-Type', 'Authorization'], // Add any custom headers your frontend sends (e.g., for JWTs)
    optionsSuccessStatus: 204 // Recommended for preflight requests
}));
// --- END CRITICAL CORS Configuration ---

app.use(express.json()); // This should be after CORS middleware

console.log('DEBUG: GOOGLE_PROJECT_ID:', process.env.GOOGLE_PROJECT_ID);
console.log('DEBUG: BIGQUERY_CLIENT_EMAIL:', process.env.BIGQUERY_CLIENT_EMAIL);
console.log('DEBUG: BIGQUERY_PRIVATE_KEY exists:', !!process.env.BIGQUERY_PRIVATE_KEY);
if (process.env.BIGQUERY_PRIVATE_KEY) {
    console.log('DEBUG: First 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.substring(0, 50));
    console.log('DEBUG: Last 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.slice(-50));
    console.log('DEBUG: Private key contains \\n:', process.env.BIGQUERY_PRIVATE_KEY.includes('\\n'));
}

const bigQueryClient = new BigQuery({
    projectId: process.env.GOOGLE_PROJECT_ID,
    credentials: {
        client_email: process.env.BIGQUERY_CLIENT_EMAIL,
        private_key: process.env.BIGQUERY_PRIVATE_KEY?.replace(/\\n/g, '\n'),
    },
    scopes: ['https://www.googleapis.com/auth/bigquery'],
});

const getAuth = () => {
    return new google.auth.GoogleAuth({
        credentials: {
            client_email: process.env.BIGQUERY_CLIENT_EMAIL,
            private_key: process.env.BIGQUERY_PRIVATE_KEY?.replace(/\\n/g, '\n'),
        },
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });
};

const ADMIN_EMAILS_BACKEND = [
    "systems@brightbraintech.com",
    "neelam.p@brightbraintech.com",
    "meghna.j@brightbraintech.com",
    "zoya.a@brightbraintech.com",
    "shweta.g@brightbraintech.com",
    "hitesh.r@brightbraintech.com"
];

app.get('/api/persons', async (req, res) => {
    try {
        // Fetches from bigQueryTable
        const query = `
            SELECT DISTINCT Responsibility
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            WHERE Responsibility IS NOT NULL AND Responsibility != ''
            ORDER BY Responsibility;
        `;
        const [rows] = await bigQueryClient.query(query);
        const persons = rows.map(row => row.Responsibility);
        console.log('Fetched distinct persons from BigQuery:', persons);
        res.status(200).json(persons);
    } catch (err) {
        console.error('Error fetching distinct persons from BigQuery:', err.message, err.stack);
        res.status(500).json({ message: 'Failed to fetch persons list.', error: err.message, stack: err.stack });
    }
});

app.get('/api/person-mappings', async (req, res) => {
    try {
        const auth = getAuth();
        const sheets = google.sheets({ version: 'v4', auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: personEmailMappingSheetId,
            range: personEmailMappingSheetRange,
        });

        const rows = response.data.values;
        if (!rows || rows.length === 0) {
            return res.status(200).json({ emailToPersonMap: {}, allAvailablePersons: [] });
        }

        const headers = rows[0];
        const dataRows = rows.slice(1);

        const emailToPersonMap = {};
        const allAvailablePersons = new Set();

        const employeeNameIndex = headers.indexOf('Current_Employes');
        const emailIndex = headers.indexOf('Emp_Emails');

        if (employeeNameIndex === -1 || emailIndex === -1) {
            throw new Error('Required columns (Current_Employes, Emp_Emails) not found in Google Sheet headers.');
        }

        dataRows.forEach(row => {
            const employeeName = row[employeeNameIndex];
            const employeeEmail = row[emailIndex];

            if (employeeName && employeeEmail) {
                emailToPersonMap[employeeEmail.toLowerCase()] = employeeName;
                allAvailablePersons.add(employeeName);
            }
        });

        const responseData = {
            emailToPersonMap: emailToPersonMap,
            allAvailablePersons: Array.from(allAvailablePersons).sort(),
        };

        console.log('Fetched person mappings from Google Sheet:', responseData);
        res.status(200).json(responseData);
    } catch (err) {
        console.error('Error fetching person-email mappings from Google Sheet:', err.message, err.stack);
        res.status(500).json({ message: 'Failed to fetch person mappings from Google Sheet.', error: err.message, stack: err.stack });
    }
});


app.get('/api/data', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit, 10) || 500;
        const offset = parseInt(req.query.offset, 10) || 0;
        const rawEmailParam = req.query.email ? req.query.email.toLowerCase() : null; 
        const requestedDelCode = req.query.delCode; // Keep as is, frontend sends correct case
        const searchTerm = req.query.searchTerm ? req.query.searchTerm.toLowerCase() : '';
        const selectedClient = req.query.selectedClient ? req.query.selectedClient.toLowerCase() : '';
        
        const isAdminRequest = ADMIN_EMAILS_BACKEND.includes(rawEmailParam);
        console.log(`Backend /api/data: Request from ${rawEmailParam}, isAdminRequest: ${isAdminRequest}`);
        console.log(`Backend /api/data: Requested delCode: ${requestedDelCode}`);
        console.log(`Backend /api/data: Search Term: "${searchTerm}", Selected Client: "${selectedClient}"`);

        if (!rawEmailParam && !isAdminRequest) {
            return res.status(400).json({ message: 'Email is required for non-admin requests' });
        }

        // --- Data fetched from bigQueryTable for main delivery/task data ---
        let baseQuery = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\``;
        let rows = [];
        // No queryParams here initially for the main block, they are defined inside if/else

        if (requestedDelCode) {
            // Logic for DeliveryDetail page (specific delCode requested)
            // REVERTED to old working logic: direct comparison without LOWER/TRIM
            let delCodeWhereClause = `WHERE DelCode_w_o__ = @requestedDelCode`;
            let delCodeParams = { requestedDelCode: requestedDelCode };

            if (isAdminRequest) {
                // Admins see all tasks for the requested delCode
                const options = {
                    query: `${baseQuery} ${delCodeWhereClause} ORDER BY Step_ID ASC;`,
                    params: delCodeParams,
                };
                [rows] = await bigQueryClient.query(options);
                console.log(`Backend /api/data (Detail View - Admin): Fetched ${rows.length} rows for delCode ${requestedDelCode}.`);

            } else {
                // Non-admins: Always get Step_ID=0 + their assigned tasks for this delCode
                
                // Query 1: Get the Step_ID=0 entry for this delCode (unconditionally)
                const queryStep0 = `${baseQuery} WHERE DelCode_w_o__ = @requestedDelCode AND Step_ID = 0;`;
                const optionsStep0 = {
                    query: queryStep0,
                    params: delCodeParams,
                };
                const [rowsStep0] = await bigQueryClient.query(optionsStep0);
                console.log(`Backend /api/data (Detail View - Non-Admin): Fetched ${rowsStep0.length} Step_ID=0 row(s) for delCode ${requestedDelCode}.`);


                // Query 2: Get all tasks (Step_ID != 0) for this delCode assigned to the user
                const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== '');
                let queryTasks = '';
                let paramsTasks = { requestedDelCode: requestedDelCode }; // Ensure delCode is here too

                if (emailsToSearch.length > 0) {
                    const emailConditions = emailsToSearch.map((email, index) => {
                        paramsTasks[`email_${index}`] = email;
                        // Keep REGEXP_CONTAINS with LOWER for 'Emails' field as it's a list and email case can vary
                        return `REGEXP_CONTAINS(LOWER(Emails), CONCAT('(^|[[:space:],])', @email_${index}, '([[:space:],]|$)'))`;
                    }).join(' OR ');
                    
                    // REVERTED to old working logic: direct comparison for DelCode_w_o__
                    queryTasks = `${baseQuery} WHERE DelCode_w_o__ = @requestedDelCode AND Step_ID != 0 AND (${emailConditions});`;
                    
                    const optionsTasks = {
                        query: queryTasks,
                        params: paramsTasks,
                    };
                    const [rowsTasks] = await bigQueryClient.query(optionsTasks);
                    console.log(`Backend /api/data (Detail View - Non-Admin): Fetched ${rowsTasks.length} assigned task row(s) for delCode ${requestedDelCode}.`);
                    rows = [...rowsStep0, ...rowsTasks]; // Combine Step_ID=0 and assigned tasks
                } else {
                    // If no valid email for non-admin, just return Step_ID=0 (if it exists)
                    rows = rowsStep0;
                    console.log(`Backend /api/data (Detail View - Non-Admin): No valid email for tasks, returned ${rows.length} Step_ID=0 row(s).`);
                }
            }
        } else { // Logic for DeliveryList page (no specific delCode)
            const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== '');
            let params = { limit, offset };
            let whereClauses = []; 

            whereClauses.push(`Step_ID = 0`); 

            if (searchTerm) {
                // Keep LOWER for search term logic as it's for flexible searching
                whereClauses.push(`(REGEXP_CONTAINS(LOWER(DelCode_w_o__), @searchTerm) OR REGEXP_CONTAINS(LOWER(Client), @searchTerm))`);
                params.searchTerm = searchTerm;
            }

            if (selectedClient) {
                // Keep LOWER for client filter as client names can vary in casing
                whereClauses.push(`LOWER(Client) = @selectedClient`);
                params.selectedClient = selectedClient;
            }
            
            // This condition ensures that records with planned timestamps are included
            whereClauses.push(`(
                (Planned_Start_Timestamp IS NOT NULL)
                OR
                (Planned_Delivery_Timestamp IS NOT NULL)
            )`);


            if (!isAdminRequest) {
                if (emailsToSearch.length === 0) {
                    return res.status(400).json({ message: 'No valid email addresses provided for non-admin request.' });
                }

                const emailConditions = emailsToSearch.map((email, index) => {
                    params[`email_${index}`] = email;
                    // Keep REGEXP_CONTAINS with LOWER for 'Emails' field
                    return `REGEXP_CONTAINS(LOWER(Emails), CONCAT('(^|[[:space:],])', @email_${index}, '([[:space:],]|$)'))`;
                }).join(' OR ');
                whereClauses.push(`(${emailConditions})`);

                const findRelevantDelCodesQuery = `
                    SELECT DISTINCT DelCode_w_o__
                    FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
                    WHERE ${whereClauses.join(' AND ')}
                `;
                console.log('Backend /api/data (List View - Non-Admin): Query to find relevant DelCodes:', findRelevantDelCodesQuery);
                console.log('Backend /api/data (List View - Non-Admin): Params for DelCode query:', params);

                const [relevantDelCodesRows] = await bigQueryClient.query({
                    query: findRelevantDelCodesQuery,
                    params: params
                });

                const relevantDelCodes = relevantDelCodesRows.map(row => row.DelCode_w_o__);
                console.log(`Backend /api/data (List View - Non-Admin): Found ${relevantDelCodes.length} relevant DelCodes:`, relevantDelCodes);

                if (relevantDelCodes.length === 0) {
                    rows = [];
                    console.log('Backend /api/data (List View - Non-Admin): No relevant DelCodes found for user, returning empty rows.');
                } else {
                    const delCodePlaceholders = relevantDelCodes.map((_, i) => `@delCode_${i}`).join(',');
                    relevantDelCodes.forEach((code, i) => {
                        params[`delCode_${i}`] = code;
                    });
                    
                    const fetchStep0ForRelevantDelCodesQuery = `
                        ${baseQuery}
                        WHERE DelCode_w_o__ IN (${delCodePlaceholders}) AND Step_ID = 0
                        ${searchTerm || selectedClient ? `AND (${whereClauses.filter(clause => !clause.includes('Step_ID') && !clause.includes('REGEXP_CONTAINS(LOWER(Emails),') && !clause.includes('(Planned_Start_Timestamp IS NOT NULL') ).join(' AND ')})` : ''}
                        ORDER BY DelCode_w_o__ LIMIT @limit OFFSET @offset;
                    `;
                    console.log('Backend /api/data (List View - Non-Admin): Query to fetch Step_ID=0 for relevant DelCodes:', fetchStep0ForRelevantDelCodesQuery);
                    console.log('Backend /api/data (List View - Non-Admin): Params for Step_ID=0 query:', params);
                    [rows] = await bigQueryClient.query({
                        query: fetchStep0ForRelevantDelCodesQuery,
                        params: params
                    });
                    console.log(`Backend /api/data (List View - Non-Admin): Fetched ${rows.length} Step_ID=0 rows after filtering by relevant DelCodes. Raw rows:`, rows);
                }
            } else { // Admin logic for DeliveryList page (no specific delCode)
                let query = `${baseQuery}`;
                if (whereClauses.length > 0) {
                    query += ` WHERE ${whereClauses.join(' AND ')}`;
                }
                query += ` ORDER BY DelCode_w_o__ LIMIT @limit OFFSET @offset;`;
                
                const options = { query: query, params: params };
                [rows] = await bigQueryClient.query(options);
                console.log(`Backend /api/data (List View - Admin): Fetched ${rows.length} Step_ID=0 rows with filters. Raw rows:`, rows);
            }
        }

        console.log('Backend /api/data: Final raw rows fetched before grouping:', rows.length);

        const groupedData = rows.reduce((acc, item) => {
            // REVERTED to old working logic: direct use of key, no trim or lower for grouping
            const key = item.DelCode_w_o__; 
            
            if (key) { 
                if (!acc[key]) {
                    acc[key] = [];
                }
                acc[key].push(item);
            }
            return acc;
        }, {});

        console.log('Backend /api/data: Final grouped data keys sent to frontend:', Object.keys(groupedData));
        res.status(200).json(groupedData);
    } catch (err) {
        console.error('Error querying BigQuery in /api/data:', err.message, err.stack);
        res.status(500).json({ message: err.message, stack: err.stack });
    }
});

app.put('/api/delivery_counts/:delCode', async (req, res) => {
    const { delCode } = req.params;
    const { newPlannedTasks, newTotalTasks } = req.body;

    if (newPlannedTasks === undefined && newTotalTasks === undefined) {
        return res.status(400).send({ error: 'At least one of newPlannedTasks or newTotalTasks must be provided.' });
    }

    try {
        let updateFields = [];
        let params = { delCode };
        let types = { delCode: 'STRING' };

        if (newPlannedTasks !== undefined) {
            updateFields.push('Planned_Tasks = @newPlannedTasks');
            params.newPlannedTasks = newPlannedTasks;
            types.newPlannedTasks = 'INT64';
        }
        if (newTotalTasks !== undefined) {
            updateFields.push('Total_Tasks = @newTotalTasks');
            params.newTotalTasks = newTotalTasks;
            types.newTotalTasks = 'INT64';
        }

        if (updateFields.length === 0) {
            return res.status(200).send({ message: 'No fields to update.' });
        }

        // Updates bigQueryTable
        // REVERTED to old working logic: direct comparison for DelCode_w_o__
        const query = `
            UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            SET
                ${updateFields.join(', ')}
            WHERE
                DelCode_w_o__ = @delCode AND Step_ID = 0;
        `;

        const options = {
            query: query,
            params: params,
            types: types
        };

        console.log('BigQuery Update Query (Delivery Counts):', query);
        console.log('BigQuery Update Params (Delivery Counts):', params);

        const [job] = await bigQueryClient.createQueryJob(options);
        await job.getQueryResults();
        res.status(200).send({ message: 'Delivery task counts updated successfully.' });
    } catch (error) {
        console.error('Error updating delivery task counts in BigQuery:', error.message, error.stack);
        res.status(500).send({ error: 'Failed to update delivery task counts.' });
    }
});


app.get('/api/per-key-per-day', async (req, res) => {
    try {
        // Fetches from bigQueryTable2
        const query = `SELECT Key, Day, Duration, Planned_Delivery_Slot, Responsibility, Duration_Unit FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
        const [rows] = await bigQueryClient.query(query);

        const groupedData = rows.reduce((acc, item) => {
            const key = item.Key;
            if (!acc[key]) {
                acc[key] = { totalDuration: 0, entries: [] };
            }
            acc[key].totalDuration += parseFloat(item.Duration) || 0;
            acc[key].entries.push(item);
            return acc;
        }, {});

        console.log("Grouped data with total duration:", groupedData);
        res.status(200).json(groupedData);
    } catch (err) {
        console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
        res.status(500).json({ message: err.message, stack: err.stack });
    }
});

app.get('/api/per-person-per-day', async (req, res) => {
    try {
        // Fetches from bigQueryTable3
        const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\``;
        const [rows] = await bigQueryClient.query(query);

        console.log("Fetched data:", rows);
        res.status(200).json(rows);
    } catch (err) {
        console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
        res.status(500).json({ message: err.message, stack: err.stack });
    }
});

app.post('/api/post', async (req, res) => {
    const {
        Key,
        Delivery_code,
        DelCode_w_o__,
        Step_ID,
        Task_Details,
        Frequency___Timeline,
        Client,
        Short_Description,
        Planned_Start_Timestamp,
        Selected_Planned_Start_Timestamp, // This field is not used in BigQuery schema directly
        Planned_Delivery_Timestamp,
        Responsibility,
        Current_Status,
        Email,
        Emails,
        Total_Tasks,
        Completed_Tasks,
        Planned_Tasks,
        Percent_Tasks_Completed,
        Created_at,
        Updated_at,
        Time_Left_For_Next_Task_dd_hh_mm_ss,
        Card_Corner_Status,
        sliders // Array of daily task data
    } = req.body;

    console.log("Backend /api/post: Received data for Key:", Key, req.body);

    if (!sliders || sliders.length === 0) {
        // If no sliders, still allow main task update/insert if Key is present
        // This path should ideally not be hit if form validation requires sliders
        console.warn("Backend /api/post: No slider data received. Only processing main task if applicable.");
    }

    try {
        // Operates on bigQueryTable (main delivery/task data)
        const checkMainTaskQuery = `SELECT Key FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` WHERE Key = @Key`;
        const checkMainTaskOptions = {
            query: checkMainTaskQuery,
            params: { Key },
            types: { Key: 'STRING' } // Key should be STRING based on your data examples
        };

        const [existingMainTasks] = await bigQueryClient.query(checkMainTaskOptions);

        if (existingMainTasks.length > 0) {
            // Update main task
            const updateMainTaskQuery = `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` SET
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
                Email = @Email,
                Emails = @Emails,
                Total_Tasks = @Total_Tasks,
                Completed_Tasks = @Completed_Tasks,
                Planned_Tasks = @Planned_Tasks,
                Percent_Tasks_Completed = @Percent_Tasks_Completed,
                Created_at = @Created_at,
                Updated_at = @Updated_at,
                Time_Left_For_Next_Task_dd_hh_mm_ss = @Time_Left_For_Next_Task_dd_hh_mm_ss,
                Card_Corner_Status = @Card_Corner_Status
                WHERE Key = @Key`;

            const updateMainTaskOptions = {
                query: updateMainTaskQuery,
                params: {
                    Key,
                    Delivery_code,
                    DelCode_w_o__: String(DelCode_w_o__),
                    Step_ID,
                    Task_Details,
                    Frequency___Timeline,
                    Client,
                    Short_Description,
                    Planned_Start_Timestamp,
                    Planned_Delivery_Timestamp,
                    Responsibility,
                    Current_Status,
                    Email,
                    Emails,
                    Total_Tasks,
                    Completed_Tasks,
                    Planned_Tasks,
                    Percent_Tasks_Completed,
                    Created_at,
                    Updated_at,
                    Time_Left_For_Next_Task_dd_hh_mm_ss,
                    Card_Corner_Status
                },
                types: {
                    Key: 'STRING', // Changed to STRING
                    Delivery_code: 'STRING',
                    DelCode_w_o__: 'STRING',
                    Step_ID: 'INT64',
                    Task_Details: 'STRING',
                    Frequency___Timeline: 'STRING',
                    Client: 'STRING',
                    Short_Description: 'STRING',
                    Planned_Start_Timestamp: 'TIMESTAMP',
                    Planned_Delivery_Timestamp: 'TIMESTAMP',
                    Responsibility: 'STRING',
                    Current_Status: 'STRING',
                    Email: 'STRING',
                    Emails: 'STRING',
                    Total_Tasks: 'INT64',
                    Completed_Tasks: 'INT64',
                    Planned_Tasks: 'INT64',
                    Percent_Tasks_Completed: 'FLOAT64',
                    Created_at: 'STRING',
                    Updated_at: 'STRING',
                    Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
                    Card_Corner_Status: 'STRING',
                }
            };
            await bigQueryClient.createQueryJob(updateMainTaskOptions);
            console.log(`Backend /api/post: Successfully updated main task with Key: ${Key}`);
        } else {
            // Insert new main task
            const insertMainTaskQuery = `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` (Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline, Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp, Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks, Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at, Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status)
            VALUES (@Key, @Delivery_code, @DelCode_w_o__, @Step_ID, @Task_Details, @Frequency___Timeline, @Client, @Short_Description, @Planned_Start_Timestamp, @Planned_Delivery_Timestamp, @Responsibility, @Current_Status, @Email, @Emails, @Total_Tasks, @Completed_Tasks, @Planned_Tasks, @Percent_Tasks_Completed, @Created_at, @Updated_at, @Time_Left_For_Next_Task_dd_hh_mm_ss, @Card_Corner_Status)`;

            const insertMainTaskOptions = {
                query: insertMainTaskQuery,
                params: {
                    Key,
                    Delivery_code,
                    DelCode_w_o__: String(DelCode_w_o__),
                    Step_ID,
                    Task_Details,
                    Frequency___Timeline,
                    Client,
                    Short_Description,
                    Planned_Start_Timestamp,
                    Planned_Delivery_Timestamp,
                    Responsibility,
                    Current_Status,
                    Email,
                    Emails,
                    Total_Tasks,
                    Completed_Tasks,
                    Planned_Tasks,
                    Percent_Tasks_Completed,
                    Created_at,
                    Updated_at,
                    Time_Left_For_Next_Task_dd_hh_mm_ss,
                    Card_Corner_Status
                },
                types: {
                    Key: 'STRING', // Changed to STRING
                    Delivery_code: 'STRING',
                    DelCode_w_o__: 'STRING',
                    Step_ID: 'INT64',
                    Task_Details: 'STRING',
                    Frequency___Timeline: 'STRING',
                    Client: 'STRING',
                    Short_Description: 'STRING',
                    Planned_Start_Timestamp: 'TIMESTAMP',
                    Planned_Delivery_Timestamp: 'TIMESTAMP',
                    Responsibility: 'STRING',
                    Current_Status: 'STRING',
                    Email: 'STRING',
                    Emails: 'STRING',
                    Total_Tasks: 'INT64',
                    Completed_Tasks: 'INT64',
                    Planned_Tasks: 'INT64',
                    Percent_Tasks_Completed: 'FLOAT64',
                    Created_at: 'STRING',
                    Updated_at: 'STRING',
                    Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
                    Card_Corner_Status: 'STRING',
                }
            };

            await bigQueryClient.createQueryJob(insertMainTaskOptions);
            console.log(`Backend /api/post: Successfully inserted new main task with Key: ${Key}`);
        }

        console.log('Backend /api/post: Processing sliders data:', sliders.length, 'entries');

        // Operates on bigQueryTable2 (Per_Key_Per_Day)
        const sliderQueriesToExecute = await Promise.all(sliders.map(async (slider) => {
            const selectQuery = {
                query: `SELECT Duration FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` WHERE Key = @Key AND Day = @Day AND Planned_Delivery_Slot=@Planned_Delivery_Slot LIMIT 1`,
                params: {
                    Key: Key, // Use Key directly (STRING)
                    Day: slider.day, // Day is STRING (YYYY-MM-DD)
                    Planned_Delivery_Slot: String(slider.slot),
                },
                types: {
                    Key: 'STRING', // Changed to STRING
                    Day: 'STRING',
                    Planned_Delivery_Slot: 'STRING',
                },
            };

            const [sliderRows] = await bigQueryClient.query(selectQuery);

            if (sliderRows.length > 0) {
                // UPDATE query for existing daily entry
                return {
                    query: `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` SET Duration = @Duration, Responsibility = @Responsibility, Duration_Unit = @Duration_Unit WHERE Key = @Key AND Day = @Day AND Planned_Delivery_Slot=@Planned_Delivery_Slot`,
                    params: {
                        Key: Key,
                        Day: slider.day,
                        Duration: Number(slider.duration),
                        Planned_Delivery_Slot: String(slider.slot),
                        Responsibility: slider.Responsibility || null, // Corrected to slider.Responsibility
                        Duration_Unit: slider.Duration_Uint || null, // Added Duration_Unit
                    },
                    types: {
                        Key: 'STRING',
                        Day: 'STRING',
                        Duration: 'INT64',
                        Planned_Delivery_Slot: 'STRING',
                        Responsibility: 'STRING',
                        Duration_Unit: 'STRING',
                    },
                };
            } else {
                // INSERT query for new daily entry
                return {
                    query: `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` (Key, Day, Duration, Planned_Delivery_Slot, Responsibility, Duration_Unit) VALUES (@Key, @Day, @Duration, @Planned_Delivery_Slot, @Responsibility, @Duration_Unit)`,
                    params: {
                        Key: Key,
                        Day: slider.day,
                        Duration: Number(slider.duration),
                        Planned_Delivery_Slot: String(slider.slot),
                        Responsibility: slider.Responsibility || null, // Corrected to slider.Responsibility
                        Duration_Unit: slider.Duration_Uint || null, // Added Duration_Unit
                    },
                    types: {
                        Key: 'STRING',
                        Day: 'STRING',
                        Duration: 'INT64',
                        Planned_Delivery_Slot: 'STRING',
                        Responsibility: 'STRING',
                        Duration_Unit: 'STRING',
                    },
                };
            }
        }));

        // Execute all collected slider queries
        for (const queryConfig of sliderQueriesToExecute) {
            await bigQueryClient.query(queryConfig);
        }
        console.log('Backend /api/post: Sliders data processed successfully.');

        res.status(200).send({ message: 'Task and slider data stored or updated successfully.' });
    } catch (error) {
        console.error('Error processing task and slider data:', error);
        // Provide more details if it's a BigQuery API error
        if (error.errors) {
            error.errors.forEach(err => console.error(`BigQuery API Error Detail: ${err.message} at ${err.location}`));
        }
        res.status(500).send({ error: `Failed to store or update task and slider data: ${error.message}` });
    }
});

app.put('/api/data/:key', async (req, res) => {
    const { key } = req.params;
    const { taskName, startDate, endDate, assignTo, status } = req.body;

    const query = `
        UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        SET Task = @Task_Details, Start_Date = @Planned_Start_Timestamp, End_Date = @Planned_Delivery_Timestamp, Assign_To = @Responsibility, Status = @Current_Status, Client=@Client, Total_Tasks = @Total_Tasks, Planned_Tasks = @Planned_Tasks, Completed_Tasks =@Completed_Tasks, Created_at = @Created_at, Updated_at = @Updated_at
        WHERE Key = @key
    `;

    const options = {
        query: query,
        params: { key: key, taskName, startDate, endDate, assignTo, status }, // Key should be string
    };

    try {
        const [job] = await bigQueryClient.createQueryJob(options);
        await job.getQueryResults();
        res.status(200).send({ message: 'Task updated successfully.' });
    } catch (error) {
        console.error('Error updating task in BigQuery:', error);
        res.status(500).send({ error: 'Failed to update task in BigQuery.' });
    }
});

app.delete('/api/data/:deliveryCode', async (req, res) => {
    const { deliveryCode } = req.params;
    console.log("hi", req.params)
    // REVERTED to old working logic: direct comparison for DelCode_w_o__
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
        console.error('Error deleting tasks from BigQuery:', error);
        res.status(500).send({ error: 'Failed to delete tasks from BigQuery.' });
    }
});


const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
