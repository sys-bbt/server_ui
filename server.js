const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Your main task table
const bigQueryTable2 = "Per_Key_Per_Day";
const bigQueryTable3 = "Per_Person_Per_Day";

const app = express();

// Define allowed origins for CORS
const allowedOrigins = [
    'http://localhost:3000', // For local development
    'https://scheduler-ui-roan.vercel.app' // Your Vercel frontend URL
];

// CORS Middleware setup
app.use(cors({
    origin: function (origin, callback) {
        // Allow requests with no origin (like mobile apps or curl requests)
        // or if the origin is in our allowed list.
        if (!origin || allowedOrigins.indexOf(origin) !== -1) {
            callback(null, true);
        } else {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            callback(new Error(msg), false);
        }
    },
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', // Allowed HTTP methods
    credentials: true, // Allow cookies to be sent
    optionsSuccessStatus: 204 // Some legacy browsers (IE11, various SmartTVs) choke on 200
}));

// Body parser middleware
app.use(express.json());

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

const ADMIN_EMAILS_BACKEND = [
    "neelam.p@brightbraintech.com",
    "meghna.j@brightbraintech.com",
    "zoya.a@brightbraintech.com",
    "shweta.g@brightbraintech.com",
    "hitesh.r@brightbraintech.com"
];

app.get('/api/persons', async (req, res) => {
    try {
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


app.get('/api/data', async (req, res) => {
    let rows; // Declare rows here so it's accessible outside the if/else
    try {
        const limit = parseInt(req.query.limit, 10) || 500;
        const offset = parseInt(req.query.offset, 10) || 0;
        const rawEmailParam = req.query.email ? req.query.email.toLowerCase() : null;
        const requestedDelCode = req.query.delCode; // This will be present for the Tasklist view
        const searchTerm = req.query.searchTerm ? req.query.searchTerm.toLowerCase() : '';
        const selectedClient = req.query.selectedClient ? req.query.selectedClient.toLowerCase() : '';

        const isAdminRequest = ADMIN_EMAILS_BACKEND.includes(rawEmailParam);
        console.log(`Backend /api/data: Request from ${rawEmailParam}, isAdminRequest: ${isAdminRequest}`);
        console.log(`Backend /api/data: Requested delCode: ${requestedDelCode || 'N/A'}`);
        console.log(`Backend /api/data: Search Term: "${searchTerm}", Selected Client: "${selectedClient}"`);


        if (!rawEmailParam && !isAdminRequest) {
            return res.status(400).json({ message: 'Email is required for non-admin requests' });
        }

        const systemResponsibilityValue = 'system';
        let baseQuery = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\``;
        let query = '';
        let params = {}; // Initialize params object here

        // Logic for DeliveryDetail page (specific delCode requested)
        if (requestedDelCode) {
            params.requestedDelCode = requestedDelCode;

            if (isAdminRequest) {
                // Admins see all tasks for the requested delCode
                query = `${baseQuery} WHERE DelCode_w_o__ = @requestedDelCode ORDER BY Step_ID ASC;`;
            } else {
                // Non-admins: Always get Step_ID=0 + their assigned tasks for this delCode
                const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== '');
                let emailConditions = '';
                if (emailsToSearch.length > 0) {
                    emailConditions = emailsToSearch.map((email, index) => {
                        params[`email_${index}`] = email;
                        return `REGEXP_CONTAINS(LOWER(Emails), CONCAT('(^|[[:space:],])', @email_${index}, '([[:space:],]|$)'))`;
                    }).join(' OR ');
                }

                // Combine user's assigned tasks and system tasks
                let combinedTaskConditions = `LOWER(Responsibility) = '${systemResponsibilityValue}'`; // Directly use string for system value in query for simplicity here
                if (emailConditions) {
                    combinedTaskConditions = `(${emailConditions}) OR (${combinedTaskConditions})`;
                }

                // Use UNION ALL to combine the Step_ID=0 entry and other assigned tasks
                query = `
                    (SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
                    WHERE DelCode_w_o__ = @requestedDelCode AND Step_ID = 0)
                    UNION ALL
                    (SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
                    WHERE DelCode_w_o__ = @requestedDelCode AND Step_ID != 0 AND (${combinedTaskConditions}))
                    ORDER BY Step_ID ASC;
                `;
            }
            console.log("Backend: DETAIL VIEW Query being sent to BigQuery:", query);
            console.log("Backend: DETAIL VIEW Query Parameters:", params);
            [rows] = await bigQueryClient.query({ query, params });
            console.log(`Backend /api/data (Detail View): Fetched ${rows.length} rows for delCode ${requestedDelCode}.`);

        } else { // Logic for DeliveryList page (general list view)
            let whereClauses = [];
            
            // For list view, we always start with Step_ID = 0
            let currentBaseQuery = `SELECT t1.* FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` t1`;

            if (isAdminRequest) {
                whereClauses.push(`t1.Step_ID = 0`);
            } else {
                // Non-admin list view: filter deliveries where the user has any assigned tasks (Step_ID != 0) OR system tasks
                const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== '');

                let emailSubqueryConditions = '';
                if (emailsToSearch.length > 0) {
                    emailSubqueryConditions = emailsToSearch.map((email, index) => {
                        params[`subquery_email_${index}`] = email; // Parameters for subquery
                        return `REGEXP_CONTAINS(LOWER(t2.Emails), CONCAT('(^|[[:space:],])', @subquery_email_${index}, '([[:space:],]|$)'))`;
                    }).join(' OR ');
                }

                let combinedSubqueryConditions = `LOWER(t2.Responsibility) = '${systemResponsibilityValue}'`;
                if (emailSubqueryConditions) {
                    combinedSubqueryConditions = `(${emailSubqueryConditions}) OR (${combinedSubqueryConditions})`;
                }

                // The main query selects Step_ID = 0 entries
                // And filters by DelCode_w_o__ that have associated tasks for the user or system
                currentBaseQuery = `
                    SELECT t1.*
                    FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` t1
                    WHERE t1.Step_ID = 0
                      AND t1.DelCode_w_o__ IN (
                        SELECT DISTINCT t2.DelCode_w_o__
                        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` t2
                        WHERE t2.Step_ID != 0
                          AND (${combinedSubqueryConditions})
                      )
                `;
            }

            // Add client filter if selected (applies to t1 for both admin/non-admin list views)
            if (selectedClient) {
                whereClauses.push(`LOWER(t1.Client) = @selectedClient`);
                params.selectedClient = selectedClient;
            }

            // Add search term filter if provided (applies to t1)
            if (searchTerm) {
                const searchCondition = `(LOWER(t1.Delivery_code) LIKE @searchTerm OR LOWER(t1.Short_Description) LIKE @searchTerm OR LOWER(t1.Client) LIKE @searchTerm)`;
                whereClauses.push(searchCondition);
                params.searchTerm = `%${searchTerm}%`;
            }

            const finalWhereClause = whereClauses.length > 0 ? ` AND ${whereClauses.join(' AND ')}` : '';
            
            // Add pagination for list view
            params.limit = limit;
            params.offset = offset;

            query = `${currentBaseQuery} ${finalWhereClause} ORDER BY t1.Created_at DESC LIMIT @limit OFFSET @offset;`;
            
            console.log("Backend: LIST VIEW Query being sent to BigQuery:", query);
            console.log("Backend: LIST VIEW Query Parameters:", params);

            [rows] = await bigQueryClient.query({ query, params });
            console.log("Backend: LIST VIEW - Data for frontend (first 5 rows):", rows.slice(0, 5));
            console.log(`Backend /api/data (List View): Fetched ${rows.length} rows.`);
        }
        res.json(rows);
    } catch (error) {
        console.error("Error fetching data from BigQuery (general):", error);
        res.status(500).json({ error: 'Failed to fetch delivery data.', details: error.message });
    }
});


// Endpoint to handle updating task status
app.put('/api/update-task-status', async (req, res) => {
    const { key, startDate, endDate, hours, deliverySlot, personResponsible, numberOfDays } = req.body;

    if (!key) {
        return res.status(400).json({ error: 'Task Key is required.' });
    }

    try {
        // 1. Update the main task table (bigQueryTable)
        const updateTaskQuery = `
            UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
            SET
                Planned_Start_Timestamp = @startDate,
                Planned_Delivery_Timestamp = @endDate,
                Responsibility = @personResponsible,
                Updated_at = CURRENT_TIMESTAMP()
            WHERE
                Key = @key;
        `;

        const taskUpdateOptions = {
            query: updateTaskQuery,
            params: {
                key: parseInt(key),
                startDate: startDate ? new Date(startDate) : null,
                endDate: endDate ? new Date(endDate) : null,
                personResponsible: personResponsible || null,
            },
        };

        await bigQueryClient.query(taskUpdateOptions);
        console.log(`Task with Key ${key} updated in ${bigQueryTable}.`);


        // 2. Upsert into Per_Key_Per_Day table (bigQueryTable2)
        // Delete existing entries for this key to ensure clean upsert
        const deletePerKeyQuery = `
            DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`
            WHERE Key = @key;
        `;
        await bigQueryClient.query({ query: deletePerKeyQuery, params: { key: parseInt(key) } });
        console.log(`Existing entries for Key ${key} deleted from ${bigQueryTable2}.`);

        // Insert new entries for Per_Key_Per_Day
        for (const day of Object.keys(hours)) {
            const duration = hours[day]; // Duration in minutes for that day
            const mergePerKeyQuery = `
                INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` (Key, Day, Duration_In_Minutes)
                VALUES (@key, @day, @duration);
            `;
            const perKeyOptions = {
                query: mergePerKeyQuery,
                params: {
                    key: parseInt(key),
                    day: day,
                    duration: parseInt(duration),
                },
            };
            await bigQueryClient.query(perKeyOptions);
        }
        console.log('Per-key-per-day entries upserted successfully.');

        // 3. Upsert into Per_Person_Per_Day table (bigQueryTable3)
        // This is more complex as it requires summing durations for a person per day.
        // It's generally better to calculate this on the fly or in a materialized view
        // if performance is critical for reporting. For updates, we'll re-calculate for simplicity.

        // Get all tasks for the personResponsible for the relevant days to re-calculate total duration for those days
        const getPersonTasksQuery = `
            SELECT
                t2.Responsibility,
                t2.Day,
                t2.Duration_In_Minutes
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` AS t2
            JOIN \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` AS t1
            ON t1.Key = t2.Key
            WHERE t1.Responsibility = @personResponsible
            AND t2.Day IN UNNEST(@daysArray);
        `;
        const daysArray = Object.keys(hours); // Days affected by this update
        const [personDailyDurations] = await bigQueryClient.query({
            query: getPersonTasksQuery,
            params: { personResponsible: personResponsible, daysArray: daysArray },
        });

        const summedDurations = {};
        personDailyDurations.forEach(row => {
            if (!summedDurations[row.Day]) {
                summedDurations[row.Day] = 0;
            }
            summedDurations[row.Day] += row.Duration_In_Minutes;
        });

        for (const day in summedDurations) {
            const totalDurationForDay = summedDurations[day];
            const mergePerPersonQuery = `
                MERGE \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\` AS T
                USING (SELECT
                    @personResponsible AS Responsibility,
                    @day AS Day,
                    @totalDurationForDay AS Duration_In_Minutes
                ) S
                ON T.Responsibility = S.Responsibility AND T.Day = S.Day
                WHEN MATCHED THEN
                    UPDATE SET Duration_In_Minutes = S.Duration_In_Minutes
                WHEN NOT MATCHED THEN
                    INSERT (Responsibility, Day, Duration_In_Minutes)
                    VALUES (S.Responsibility, S.Day, S.Duration_In_Minutes);
            `;
            const perPersonOptions = {
                query: mergePerPersonQuery,
                params: {
                    personResponsible: personResponsible,
                    day: day,
                    totalDurationForDay: totalDurationForDay,
                },
            };
            await bigQueryClient.query(perPersonOptions);
        }
        console.log('Per-person-per-day entries upserted successfully.');

        res.status(200).json({ message: 'Task and schedules updated successfully' });

    } catch (error) {
        console.error("Error during BigQuery operation (PUT /api/update-task-status):", error);
        if (error.errors && error.errors[0] && error.errors[0].reason) {
            console.error("BigQuery specific error reason:", error.errors[0].reason);
        }
        res.status(500).json({ error: `Failed to update task: ${error.message}` });
    }
});

// Delete Task from BigQuery
app.delete('/api/data/:deliveryCode', async (req, res) => {
    const { deliveryCode } = req.params;
    console.log("Attempting to delete tasks for DelCode_w_o__:", deliveryCode);
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
        console.log('Successfully deleted tasks for DelCode_w_o__:', deliveryCode);
        res.status(200).send({ message: 'All tasks with the specified delivery code were deleted successfully.' });
    } catch (error) {
        console.error('Error deleting tasks from BigQuery:', error);
        res.status(500).send({ error: 'Failed to delete tasks from BigQuery.' });
    }
});


// Endpoint to fetch data for Per_Key_Per_Day (bigQueryTable2)
app.get('/api/per-key-per-day', async (req, res) => {
    try {
        const query = `
            SELECT Key, Day, Duration_In_Minutes
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`;
        `;
        const [rows] = await bigQueryClient.query(query);
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching per-key-per-day data from BigQuery:', error);
        res.status(500).json({ error: 'Failed to fetch per-key-per-day data from BigQuery.' });
    }
});

// Endpoint to fetch data for Per_Person_Per_Day (bigQueryTable3)
app.get('/api/per-person-per-day', async (req, res) => {
    try {
        const query = `
            SELECT Responsibility, Day, Duration_In_Minutes
            FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\`;
        `;
        const [rows] = await bigQueryClient.query(query);
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching per-person-per-day data from BigQuery:', error);
        res.status(500).json({ error: 'Failed to fetch per-person-per-day data from BigQuery.' });
    }
});


const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
