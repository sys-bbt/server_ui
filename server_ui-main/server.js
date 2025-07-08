const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Your main task table
const bigQueryTable2 = "Per_Key_Per_Day"; // This table will now include Responsibility
const bigQueryTable3 = "Per_Person_Per_Day";

const app = express();

// --- CORS Configuration ---
const allowedOrigins = [
    'https://scheduler-ui-roan.vercel.app', // Your Vercel frontend URL
    'http://localhost:3000', // For local development if you use this port
    'http://localhost:3001' // If your frontend is sometimes on 3001 for local testing
];

app.use(cors({
    origin: function (origin, callback) {
        if (!origin) return callback(null, true);
        if (allowedOrigins.indexOf(origin) === -1) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
    optionsSuccessStatus: 204
}));
// --- END CORS Configuration ---

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
    "systems@brightbraintech.com",
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
    try {
        const limit = parseInt(req.query.limit, 10) || 500;
        const offset = parseInt(req.query.offset, 10) || 0;
        const rawEmailParam = req.query.email ? req.query.email.toLowerCase() : null; 
        const requestedDelCode = req.query.delCode;
        
        const isAdminRequest = ADMIN_EMAILS_BACKEND.includes(rawEmailParam);
        console.log(`Backend /api/data: Request from ${rawEmailParam}, isAdminRequest: ${isAdminRequest}`);
        console.log(`Backend /api/data: Requested delCode: ${requestedDelCode}`);


        if (!rawEmailParam && !isAdminRequest) {
            return res.status(400).json({ message: 'Email is required for non-admin requests' });
        }

        let baseQuery = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\``;
        let rows = [];

        if (requestedDelCode) {
            let delCodeWhereClause = `WHERE DelCode_w_o__ = @requestedDelCode`;
            let delCodeParams = { requestedDelCode: requestedDelCode };

            if (isAdminRequest) {
                const options = {
                    query: `${baseQuery} ${delCodeWhereClause} ORDER BY Step_ID ASC;`,
                    params: delCodeParams,
                };
                [rows] = await bigQueryClient.query(options);
                console.log(`Backend /api/data (Detail View - Admin): Fetched ${rows.length} rows for delCode ${requestedDelCode}.`);
            } else {
                const queryStep0 = `${baseQuery} WHERE DelCode_w_o__ = @requestedDelCode AND Step_ID = 0;`;
                const optionsStep0 = {
                    query: queryStep0,
                    params: delCodeParams,
                };
                const [rowsStep0] = await bigQueryClient.query(optionsStep0);
                console.log(`Backend /api/data (Detail View - Non-Admin): Fetched ${rowsStep0.length} Step_ID=0 row(s) for delCode ${requestedDelCode}.`);

                const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== '');
                let queryTasks = '';
                let paramsTasks = { requestedDelCode: requestedDelCode };

                if (emailsToSearch.length > 0) {
                    const emailConditions = emailsToSearch.map((email, index) => {
                        paramsTasks[`email_${index}`] = email;
                        return `REGEXP_CONTAINS(LOWER(Emails), CONCAT('(^|[^a-z0-9.@_-])', @email_${index}, '([^a-z0-9.@_-]|$)'))`;
                    }).join(' OR ');
                    
                    queryTasks = `${baseQuery} WHERE DelCode_w_o__ = @requestedDelCode AND Step_ID != 0 AND (${emailConditions});`;
                    
                    const optionsTasks = {
                        query: queryTasks,
                        params: paramsTasks,
                    };
                    const [rowsTasks] = await bigQueryClient.query(optionsTasks);
                    console.log(`Backend /api/data (Detail View - Non-Admin): Fetched ${rowsTasks.length} assigned task row(s) for delCode ${requestedDelCode}.`);
                    rows = [...rowsStep0, ...rowsTasks];
                } else {
                    rows = rowsStep0;
                    console.log(`Backend /api/data (Detail View - Non-Admin): No valid email for tasks, returned ${rows.length} Step_ID=0 row(s).`);
                }
            }
        } else {
            const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== '');
            let params = { limit, offset };

            if (!isAdminRequest) {
                if (emailsToSearch.length === 0) {
                    return res.status(400).json({ message: 'No valid email addresses provided for non-admin request.' });
                }

                const emailConditions = emailsToSearch.map((email, index) => {
                    params[`email_${index}`] = email;
                    return `REGEXP_CONTAINS(LOWER(Emails), CONCAT('(^|[^a-z0-9.@_-])', @email_${index}, '([^a-z0-9.@_-]|$)'))`;
                }).join(' OR ');

                const findRelevantDelCodesQuery = `
                    SELECT DISTINCT DelCode_w_o__
                    FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
                    WHERE (${emailConditions})
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
                let whereClauses = [];
                whereClauses.push(`Step_ID = 0`);
                 whereClauses.push(`(
                    (Planned_Start_Timestamp IS NULL AND Planned_Delivery_Timestamp IS NULL)
                    OR
                    (Planned_Start_Timestamp IS NOT NULL AND Planned_Delivery_Timestamp IS NOT NULL)
                    OR
                    (Planned_Start_Timestamp IS NOT NULL AND Planned_Delivery_Timestamp IS NULL)
                    OR
                    (Planned_Start_Timestamp IS NULL AND Planned_Delivery_Timestamp IS NOT NULL)
                )`);

                let query = `${baseQuery} WHERE ` + whereClauses.join(' AND ');
                query += ` ORDER BY DelCode_w_o__ LIMIT @limit OFFSET @offset;`;
                const options = { query: query, params: params };
                [rows] = await bigQueryClient.query(options);
                console.log(`Backend /api/data (List View - Admin): Fetched ${rows.length} Step_ID=0 rows. Raw rows:`, rows);
            }
        }

        console.log('Backend /api/data: Final raw rows fetched before grouping:', rows.length);

        const groupedData = rows.reduce((acc, item) => {
            const key = item.DelCode_w_o__;
            if (!acc[key]) {
                acc[key] = [];
            }
            acc[key].push(item);
            return acc;
        }, {});

        console.log('Backend /api/data: Final grouped data keys sent to frontend:', Object.keys(groupedData));
        res.status(200).json(groupedData);
    } catch (err) {
        console.error('Error querying BigQuery in /api/data:', err.message, err.stack);
        res.status(500).json({ message: err.message, stack: err.stack });
    }
});

// NEW API ENDPOINT: To update Planned_Tasks and Total_Tasks for a specific delivery
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
        // --- UPDATED: Select Responsibility as well ---
        const query = `SELECT Key, day, duration, Planned_Delivery_Slot, Responsibility FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
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
        Selected_Planned_Start_Timestamp,
        Planned_Delivery_Timestamp,
        Responsibility, // Main task Responsibility
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
        sliders // Each slider now contains its own personResponsible
    } = req.body;

    console.log("Backend /api/post: Received data for Key:", Key, req.body);

    if (!sliders || sliders.length === 0) {
        return res.status(400).send({ error: 'Slider data is mandatory.' });
    }

    try {
        const checkQuery = `SELECT Key FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` WHERE Key = @Key`;
        const checkOptions = {
            query: checkQuery,
            params: { Key },
            types: { Key: 'INT64' }
        };

        const [existingTasks] = await bigQueryClient.query(checkOptions);

        if (existingTasks.length > 0) {
            // Update main task table
            const updateQuery = `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` SET
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

            const updateOptions = {
                query: updateQuery,
                params: {
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
                    Key: 'INT64',
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
            await bigQueryClient.createQueryJob(updateOptions);
            console.log(`Backend /api/post: Successfully updated main task with Key: ${Key}`);
        } else {
            // Insert logic (unchanged)
            const insertQuery = `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` (Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline, Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp, Responsibility, Current_Status,Email, Total_Tasks, Completed_Tasks, Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at, Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status)
            VALUES (@Key, @Delivery_code, @DelCode_w_o__, @Step_ID, @Task_Details, @Frequency___Timeline, @Client, @Short_description, @Planned_Start_Timestamp, @Planned_Delivery_Timestamp, @Responsibility, @Current_Status,@Email, @Total_Tasks, @Completed_Tasks, @Planned_Tasks, @Percent_Tasks_Completed, @Created_at, @Updated_at, @Time_Left_For_Next_Task_dd_hh_mm_ss, @Card_Corner_Status)`;

            const insertOptions = {
                query: insertQuery,
                params: {
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
                    Key: 'INT64',
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

            await bigQueryClient.createQueryJob(insertOptions);
            console.log(`Backend /api/post: Successfully inserted new task with Key: ${Key}`);
        }

        console.log('Backend /api/post: Processing sliders data:', sliders.length, 'entries');

        const insertOrUpdateSliderQueries = await Promise.all(sliders.map(async (slider) => {
            const selectQuery = {
                query: `SELECT duration FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` WHERE Key = @Key AND day = @day AND Planned_Delivery_Slot=@Planned_Delivery_Slot LIMIT 1`,
                params: {
                    Key: Number(Key),
                    day: slider.day,
                    Planned_Delivery_Slot: slider.slot,
                },
                types: {
                    Key: 'INT64',
                    day: 'STRING',
                    Planned_Delivery_Slot: 'STRING',
                },
            };

            const [sliderRows] = await bigQueryClient.query(selectQuery);

            if (sliderRows.length > 0) {
                // --- UPDATED: Include Responsibility in UPDATE for Per_Key_Per_Day ---
                return {
                    query: `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` SET duration = @duration, Responsibility = @Responsibility WHERE Key = @Key AND day = @day AND Planned_Delivery_Slot=@Planned_Delivery_Slot`,
                    params: {
                        Key: Number(Key),
                        day: slider.day,
                        duration: Number(slider.duration),
                        Planned_Delivery_Slot: slider.slot,
                        Responsibility: slider.personResponsible || null, // Use from slider data
                    },
                    types: {
                        Key: 'INT64',
                        day: 'STRING',
                        duration: 'INT64',
                        Planned_Delivery_Slot: 'STRING',
                        Responsibility: 'STRING', // Define type for new column
                    },
                };
            } else {
                // --- UPDATED: Include Responsibility in INSERT for Per_Key_Per_Day ---
                return {
                    query: `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` (Key, day, duration, Planned_Delivery_Slot, Responsibility) VALUES (@Key, @day, @duration, @Planned_Delivery_Slot, @Responsibility)`,
                    params: {
                        Key: Number(Key),
                        day: slider.day,
                        duration: Number(slider.duration),
                        Planned_Delivery_Slot: slider.slot,
                        Responsibility: slider.personResponsible || null, // Use from slider data
                    },
                    types: {
                        Key: 'INT64',
                        day: 'STRING',
                        duration: 'INT64',
                        Planned_Delivery_Slot: 'STRING',
                        Responsibility: 'STRING', // Define type for new column
                    },
                };
            }
        }));

        await Promise.all(
            insertOrUpdateSliderQueries.map(async (queryOption) => {
                await bigQueryClient.createQueryJob(queryOption);
            })
        );
        console.log('Backend /api/post: Sliders data processed successfully.');

        res.status(200).send({ message: 'Task and slider data stored or updated successfully.' });
    } catch (error) {
        console.error('Error processing task and slider data:', error);
        res.status(500).send({ error: 'Failed to store or update task and slider data.' });
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
        params: { key: parseInt(key), taskName, startDate, endDate, assignTo, status },
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
