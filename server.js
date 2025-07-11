const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');
const { google } = require('googleapis'); // For Google Sheets
const Joi = require('joi'); // For validation

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE; // Your main task table
const bigQueryTable2 = "Per_Key_Per_Day"; // For per-key per-day tracking
const bigQueryTable3 = "Per_Person_Per_Day"; // For per-person per-day tracking

const app = express();

// Middleware setup
app.use(cors());
app.use(express.json());

// Debug logging for BigQuery credentials (from HEAD)
console.log('DEBUG: GOOGLE_PROJECT_ID:', process.env.GOOGLE_PROJECT_ID);
console.log('DEBUG: BIGQUERY_CLIENT_EMAIL:', process.env.BIGQUERY_CLIENT_EMAIL);
console.log('DEBUG: BIGQUERY_PRIVATE_KEY exists:', !!process.env.BIGQUERY_PRIVATE_KEY);
if (process.env.BIGQUERY_PRIVATE_KEY) {
    console.log('DEBUG: First 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.substring(0, 50));
    console.log('DEBUG: Last 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.slice(-50));
    console.log('DEBUG: Private key contains \\n:', process.env.BIGQUERY_PRIVATE_KEY.includes('\\n'));
}

// BigQuery Client Setup
const bigQueryClient = new BigQuery({
    projectId: projectId,
    credentials: {
        client_email: process.env.BIGQUERY_CLIENT_EMAIL,
        private_key: process.env.BIGQUERY_PRIVATE_KEY.replace(/\\n/g, '\n'),
    },
});

// Google Sheets Client Setup
const sheetsAuth = new google.auth.GoogleAuth({
    credentials: {
        client_email: process.env.GOOGLE_SHEET_CLIENT_EMAIL,
        private_key: process.env.GOOGLE_SHEET_PRIVATE_KEY.replace(/\\n/g, '\n'),
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'], // Read-only scope
});

let sheetsClient;

async function initializeSheetsClient() {
    try {
        sheetsClient = google.sheets({ version: 'v4', auth: sheetsAuth });
        console.log('Google Sheets client initialized successfully.');
    } catch (error) {
        console.error('Error initializing Google Sheets client:', error);
        // Exit process or handle error appropriately
        process.exit(1);
    }
}
initializeSheetsClient();


// Joi Validation Schema for update-task-status
const updateTaskSchema = Joi.object({
    key: Joi.number().integer().required(),
    taskName: Joi.string().required(),
    startDate: Joi.date().iso().allow(null, ''),
    endDate: Joi.date().iso().allow(null, ''),
    assignTo: Joi.string().required(),
    status: Joi.string().valid('Scheduled', 'In Progress', 'Paused', 'Completed').required(),
    actualHours: Joi.number().integer().min(0).allow(null), // Assuming this is for Task_Duration_In_Minutes, make it optional as it might be for a different type of update
    // If you need more specific validation for 'actualHours' based on task status, add it here
});

// Endpoint to fetch people mapping from Google Sheet
app.get('/api/people-mapping', async (req, res) => {
    try {
        if (!sheetsClient) {
            await initializeSheetsClient(); // Ensure client is initialized
        }

        const sheetId = process.env.GOOGLE_SHEET_ID;
        const sheetRange = process.env.GOOGLE_SHEET_RANGE || 'Sheet1!A:Z'; // Default range

        const response = await sheetsClient.spreadsheets.values.get({
            spreadsheetId: sheetId,
            range: sheetRange,
        });

        const rows = response.data.values;
        if (!rows || rows.length === 0) {
            return res.status(200).json([]); // Return empty array if no data
        }

        // Assuming the first row is headers
        const headers = rows[0];
        const peopleMapping = rows.slice(1).map(row => {
            let obj = {};
            headers.forEach((header, index) => {
                obj[header] = row[index];
            });
            return obj;
        });

        res.status(200).json(peopleMapping);
    } catch (error) {
        console.error('Error fetching people mapping from Google Sheet:', error);
        res.status(500).json({ error: 'Failed to fetch people mapping data from Google Sheet.' });
    }
});

// Fetch all deliveries or filter by client/search term with pagination
app.get('/api/data', async (req, res) => {
    const { page = 0, limit = 10, client, search } = req.query; // Default page 0, limit 10

    const offset = parseInt(page) * parseInt(limit);

    let query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\``;
    let conditions = [];
    let params = {};

    if (client) {
        conditions.push('Client = @client');
        params.client = client;
    }

    if (search) {
        // Example: Search across multiple text fields for the search term
        conditions.push(
            `(LOWER(Task_Details) LIKE LOWER(@search) OR LOWER(DelCode_w_o__) LIKE LOWER(@search))`
        );
        params.search = `%${search}%`;
    }

    if (conditions.length > 0) {
        query += ' WHERE ' + conditions.join(' AND ');
    }

    query += ` ORDER BY initiated DESC LIMIT @limit OFFSET @offset`; // Order by initiated date descending
    params.limit = parseInt(limit);
    params.offset = offset;

    const options = {
        query: query,
        params: params,
        location: 'US', // Specify your dataset location if not default
    };

    try {
        const [job] = await bigQueryClient.createQueryJob(options);
        const [rows] = await job.getQueryResults();
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching deliveries from BigQuery:', error);
        res.status(500).json({ error: 'Failed to fetch deliveries from BigQuery.' });
    }
});

// Endpoint to fetch tasks by Key for per-key-per-day data
app.get('/api/per-key-per-day', async (req, res) => {
    const { key } = req.query; // Expecting 'key' as a query parameter

    if (!key) {
        return res.status(400).json({ error: 'Task Key is required.' });
    }

    const query = `
        SELECT *
        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`
        WHERE Key = @key
    `;

    const options = {
        query: query,
        params: { key: parseInt(key) },
        location: 'US',
    };

    try {
        const [job] = await bigQueryClient.createQueryJob(options);
        const [rows] = await job.getQueryResults();
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching per-key-per-day data from BigQuery:', error);
        res.status(500).json({ error: 'Failed to fetch per-key-per-day data.' });
    }
});


// Update Task Status (including Joi validation and Per_Key_Per_Day / Per_Person_Per_Day updates)
app.put('/api/update-task-status', async (req, res) => {
    const { key, taskName, startDate, endDate, assignTo, status, actualHours, newSchedules } = req.body;

    // Validate request body using Joi
    const { error } = updateTaskSchema.validate({
        key, taskName, startDate, endDate, assignTo, status, actualHours
    });

    if (error) {
        return res.status(400).send({ error: error.details[0].message });
    }

    // Update main task table
    const updateMainTaskQuery = `
        UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        SET
            Task_Details = @taskName,
            Planned_Start_Timestamp = @startDate,
            Planned_Delivery_Timestamp = @endDate,
            Responsibility = @assignTo,
            Status = @status,
            Task_Duration_In_Minutes = @actualHours
        WHERE
            Key = @key
    `;

    const mainTaskOptions = {
        query: updateMainTaskQuery,
        params: {
            key: parseInt(key),
            taskName,
            startDate,
            endDate,
            assignTo,
            status,
            actualHours: actualHours !== null ? parseInt(actualHours) : null // Ensure integer or null
        },
    };

    try {
        // Execute main task update
        const [mainJob] = await bigQueryClient.createQueryJob(mainTaskOptions);
        await mainJob.getQueryResults();
        console.log(`Task with Key ${key} updated in main table.`);

        // Process and upsert into Per_Key_Per_Day table if newSchedules are provided
        if (newSchedules && Array.isArray(newSchedules) && newSchedules.length > 0) {
            for (const schedule of newSchedules) {
                const { date, duration } = schedule;
                const mergePerKeyQuery = `
                    MERGE \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` T
                    USING (SELECT @key as Key, @date as Day, @duration as Duration_In_Minutes) S
                    ON T.Key = S.Key AND T.Day = S.Day
                    WHEN MATCHED THEN
                        UPDATE SET Duration_In_Minutes = S.Duration_In_Minutes
                    WHEN NOT MATCHED THEN
                        INSERT (Key, Day, Duration_In_Minutes)
                        VALUES (S.Key, S.Day, S.Duration_In_Minutes);
                `;
                const perKeyOptions = {
                    query: mergePerKeyQuery,
                    params: { key: parseInt(key), date, duration: parseInt(duration) },
                };
                await bigQueryClient.query(perKeyOptions);
            }
            console.log('Per-key-per-day entries upserted successfully.');
        }

        // Process and upsert into Per_Person_Per_Day table based on newSchedules and assignTo
        // This logic assumes `newSchedules` will contain dates and durations needed to update per-person-per-day sums.
        // You might need to refine this based on how you calculate per-person-per-day.
        if (newSchedules && Array.isArray(newSchedules) && newSchedules.length > 0 && assignTo) {
            // Aggregate duration per person per day from newSchedules for the assigned person
            const perPersonDailyDurations = {};
            newSchedules.forEach(schedule => {
                if (perPersonDailyDurations[schedule.date]) {
                    perPersonDailyDurations[schedule.date] += parseInt(schedule.duration);
                } else {
                    perPersonDailyDurations[schedule.date] = parseInt(schedule.duration);
                }
            });

            for (const day in perPersonDailyDurations) {
                const mergePerPersonQuery = `
                    MERGE \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\` T
                    USING (SELECT @assignTo as Responsibility, @day as Day, @duration as Duration_In_Minutes) S
                    ON T.Responsibility = S.Responsibility AND T.Day = S.Day
                    WHEN MATCHED THEN
                        UPDATE SET Duration_In_Minutes = S.Duration_In_Minutes
                    WHEN NOT MATCHED THEN
                        INSERT (Responsibility, Day, Duration_In_Minutes)
                        VALUES (S.Responsibility, S.Day, S.Duration_In_Minutes);
                `;
                const perPersonOptions = {
                    query: mergePerPersonQuery,
                    params: { assignTo, day, duration: perPersonDailyDurations[day] },
                };
                await bigQueryClient.query(perPersonOptions);
            }
            console.log('Per-person-per-day entries upserted successfully.');
        }


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
    console.log("Attempting to delete delivery with code:", deliveryCode);

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


const PORT = process.env.PORT || 5000; // Use 5000 as default as per common practice
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});