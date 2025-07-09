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

// Middleware setup
app.use(cors());
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
    try {
        const limit = parseInt(req.query.limit, 10) || 500;
        const offset = parseInt(req.query.offset, 10) || 0;
        const rawEmailParam = req.query.email ? req.query.email.toLowerCase() : null; 
        const requestedDelCode = req.query.delCode;
        const searchTerm = req.query.searchTerm ? req.query.searchTerm.toLowerCase() : ''; // Get search term
        const selectedClient = req.query.selectedClient ? req.query.selectedClient.toLowerCase() : ''; // Get selected client
        
        const isAdminRequest = ADMIN_EMAILS_BACKEND.includes(rawEmailParam);
        console.log(`Backend /api/data: Request from ${rawEmailParam}, isAdminRequest: ${isAdminRequest}`);
        console.log(`Backend /api/data: Requested delCode: ${requestedDelCode}`);
        console.log(`Backend /api/data: Search Term: "${searchTerm}", Selected Client: "${selectedClient}"`);


        if (!rawEmailParam && !isAdminRequest) {
            return res.status(400).json({ message: 'Email is required for non-admin requests' });
        }

        let baseQuery = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\``;
        let rows = [];

        if (requestedDelCode) {
            // Logic for DeliveryDetail page (specific delCode requested)
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
                const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== ''); // Already lowercase
                let queryTasks = '';
                let paramsTasks = { requestedDelCode: requestedDelCode };

                if (emailsToSearch.length > 0) {
                    const emailConditions = emailsToSearch.map((email, index) => {
                        paramsTasks[`email_${index}`] = email;
                        // Changed regex to be more robust for separators (commas, spaces, or start/end of string)
                        return `REGEXP_CONTAINS(LOWER(Emails), CONCAT('(^|[[:space:],])', @email_${index}, '([[:space:],]|$)'))`;
                    }).join(' OR ');
                    
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
        } else {
            // Corrected Logic for DeliveryList page (no specific delCode requested)
            const emailsToSearch = rawEmailParam.split(',').map(email => email.trim().toLowerCase()).filter(email => email !== ''); // Already lowercase
            let params = { limit, offset };
            let whereClauses = []; // Initialize whereClauses here

            // Always filter for Step_ID = 0 for the list view
            whereClauses.push(`Step_ID = 0`);

            // Add search term filter
            if (searchTerm) {
                whereClauses.push(`(REGEXP_CONTAINS(LOWER(DelCode_w_o__), @searchTerm) OR REGEXP_CONTAINS(LOWER(Client), @searchTerm))`);
                params.searchTerm = searchTerm;
            }

            // Add selected client filter
            if (selectedClient) {
                whereClauses.push(`LOWER(Client) = @selectedClient`);
                params.selectedClient = selectedClient;
            }

            if (isAdminRequest) { // ADMIN list view logic
                // Admins see all Step_ID=0 deliveries with search/client filters
                let query = `${baseQuery} WHERE ${whereClauses.join(' AND ')}`;
                query += ` ORDER BY DelCode_w_o__ LIMIT @limit OFFSET @offset;`;
                
                const options = { query: query, params: params };
                [rows] = await bigQueryClient.query(options);
                console.log(`Backend /api/data (List View - Admin): Fetched ${rows.length} Step_ID=0 rows with filters. Raw rows:`, rows);
            } else { // NON-ADMIN list view logic
                if (emailsToSearch.length === 0) {
                    return res.status(400).json({ message: 'No valid email addresses provided for non-admin request.' });
                }

                const emailConditions = emailsToSearch.map((email, index) => {
                    params[`email_${index}`] = email;
                    return `REGEXP_CONTAINS(LOWER(Emails), CONCAT('(^|[[:space:],])', @email_${index}, '([[:space:],]|$)'))`;
                }).join(' OR ');

                // Query to find relevant DelCodes (where user is part of any task AND matches search/client filters)
                const findRelevantDelCodesQuery = `
                    SELECT DISTINCT DelCode_w_o__
                    FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
                    WHERE (${emailConditions})
                    ${searchTerm || selectedClient ? `AND (${whereClauses.filter(clause => !clause.includes('Step_ID')).join(' AND ')})` : ''}
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
                    rows = []; // No relevant workflows found
                    console.log('Backend /api/data (List View - Non-Admin): No relevant DelCodes found for user, returning empty rows.');
                } else {
                    // Now, fetch the Step_ID = 0 entry for each of these relevant DelCodes
                    const delCodePlaceholders = relevantDelCodes.map((_, i) => `@delCode_${i}`).join(',');
                    relevantDelCodes.forEach((code, i) => {
                        params[`delCode_${i}`] = code;
                    });

                    const fetchStep0ForRelevantDelCodesQuery = `
                        ${baseQuery}
                        WHERE DelCode_w_o__ IN (${delCodePlaceholders}) AND Step_ID = 0
                        ${searchTerm || selectedClient ? `AND (${whereClauses.filter(clause => !clause.includes('Step_ID')).join(' AND ')})` : ''}
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

app.get('/api/per-key-per-day', async (req, res) => {
    try {
        const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
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
        sliders
    } = req.body;

    console.log("Hi", req.body);

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
                Total_Tasks = @Total_Tasks,
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
        } else {
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
        }

        console.log('Received sliders data:', sliders);

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
                return {
                    query: `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` SET duration = @duration WHERE Key = @Key AND day = @day AND Planned_Delivery_Slot=@Planned_Delivery_Slot`,
                    params: {
                        Key: Number(Key),
                        day: slider.day,
                        duration: Number(slider.duration),
                        Planned_Delivery_Slot: slider.slot,
                    },
                    types: {
                        Key: 'INT64',
                        day: 'STRING',
                        duration: 'INT64',
                        Planned_Delivery_Slot: 'STRING',
                    },
                };
            } else {
                return {
                    query: `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` (Key, day, duration,Planned_Delivery_Slot) VALUES (@Key, @day, @duration,@Planned_Delivery_Slot)`,
                    params: {
                        Key: Number(Key),
                        day: slider.day,
                        duration: Number(slider.duration),
                        Planned_Delivery_Slot: slider.slot
                    },
                    types: {
                        Key: 'INT64',
                        day: 'STRING',
                        duration: 'INT64',
                        Planned_Delivery_Slot: 'STRING',
                    },
                };
            }
        }));

        await Promise.all(
            insertOrUpdateSliderQueries.map(async (queryOption) => {
                await bigQueryClient.createQueryJob(queryOption);
            })
        );

        res.status(200).send({ message: 'Task and slider data stored or updated successfully.' });
    } catch (error) {
        console.error('Error processing task and slider data:', error);
        res.status(500).send({ error: 'Failed to store or update task and slider data.' });
    }
});

// Update Task in BigQuery
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
