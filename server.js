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


// New endpoint to fetch people mapping (already added in previous turn)
app.get('/api/people-mapping', async (req, res) => {
    const query = `
        SELECT Current_Employes, Emp_Emails
        FROM \`${projectId}.${bigQueryDataset}.People_To_Email_Mapping\`
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

// Modified /api/data route (GET all tasks with filtering for non-admins)
app.get('/api/data', async (req, res) => {
    const userEmail = req.query.email; // Get email from query parameter
    let query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\``;
    let params = {};

    // Check if userEmail is provided and if it's NOT an admin email
    if (userEmail && !ADMIN_EMAILS_BACKEND.includes(userEmail)) {
        // Non-admin user: show tasks assigned to them OR tasks assigned to 'System'
        // OR any task belonging to a workflow where they or system are assigned.
        query += ` WHERE (Emails LIKE @userEmail OR Emails LIKE @systemEmail)
                OR DelCode_w_o__ IN (
                    SELECT DelCode_w_o__
                    FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
                    WHERE Emails LIKE @userEmail OR Emails LIKE @systemEmail
                )`;
        params = {
            userEmail: `%${userEmail}%`,
            systemEmail: `%${SYSTEM_EMAIL_FOR_GLOBAL_TASKS}%`
        };
        console.log(`Filtering tasks for non-admin user: ${userEmail} (including System tasks and associated workflows)`);
    } else if (userEmail && ADMIN_EMAILS_BACKEND.includes(userEmail)) {
        console.log(`Fetching all tasks for admin user: ${userEmail}`);
        // No WHERE clause needed for admins, query remains 'SELECT * FROM ...'
    } else {
        console.log(`Fetching all tasks (no user email provided or default behavior)`);
        // If no user email is provided, it will fetch all tasks (unfiltered)
    }

    try {
        const [rows] = await bigQueryClient.query({
            query: query,
            params: params,
            location: 'US', // Specify your BigQuery dataset location
        });
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error fetching data from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch data from BigQuery.' });
    }
});

// Existing /api/per-key-per-day route
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
    } catch (error) {
        console.error('Error fetching per-person-per-day data from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch per-person-per-day data from BigQuery.' });
    }
});

// Existing POST route
app.post('/api/post', async (req, res) => {
    const {
        Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline,
        Client, Short_Description, Planned_Start_Timestamp, Planned_Delivery_Timestamp,
        Responsibility, Current_Status, Email, Emails, Total_Tasks, Completed_Tasks,
        Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at,
        Time_Left_For_Next_Task_dd_hh_mm_ss, Card_Corner_Status, sliders
    } = req.body;

    // Convert timestamps to BigQuery compatible format if they are not null
    const formatTimestamp = (timestamp) => {
        if (!timestamp) return null;
        const momentObj = moment.utc(timestamp.replace(' UTC', ''));
        return momentObj.isValid() ? momentObj.format('YYYY-MM-DD HH:mm:ss.SSSSSS') : null;
    };

    const formattedPlannedStartTimestamp = formatTimestamp(Planned_Start_Timestamp);
    const formattedPlannedDeliveryTimestamp = formatTimestamp(Planned_Delivery_Timestamp);

    // Prepare data for the main task table update
    const mainTaskRow = {
        Key: Key,
        Delivery_code: Delivery_code,
        DelCode_w_o__: DelCode_w_o__,
        Step_ID: Step_ID,
        Task_Details: Task_Details,
        Frequency___Timeline: Frequency___Timeline,
        Client: Client,
        Short_Description: Short_Description,
        Planned_Start_Timestamp: formattedPlannedStartTimestamp,
        Planned_Delivery_Timestamp: formattedPlannedDeliveryTimestamp,
        Responsibility: Responsibility,
        Current_Status: Current_Status,
        Email: Email,
        Emails: Emails,
        Total_Tasks: Total_Tasks,
        Completed_Tasks: Completed_Tasks,
        Planned_Tasks: Planned_Tasks,
        Percent_Tasks_Completed: Percent_Tasks_Completed,
        Created_at: formatTimestamp(Created_at),
        Updated_at: formatTimestamp(Updated_at),
        Time_Left_For_Next_Task_dd_hh_mm_ss: Time_Left_For_Next_Task_dd_hh_mm_ss,
        Card_Corner_Status: Card_Corner_Status,
    };

    try {
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
            WHERE Key = @Key
        `;
        const updateMainTaskOptions = {
            query: updateMainTaskQuery,
            params: mainTaskRow,
            location: 'US',
        };
        const [mainTaskJob] = await bigQueryClient.createQueryJob(updateMainTaskOptions);
        await mainTaskJob.getQueryResults();
        console.log(`Main task with Key ${Key} updated successfully.`);

        const deletePerKeyQuery = `
            DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\`
            WHERE Key = @Key
        `;
        const deletePerKeyOptions = {
            query: deletePerKeyQuery,
            params: { Key: Key },
            location: 'US',
        };
        const [deleteJob] = await bigQueryClient.createQueryJob(deletePerKeyOptions);
        await deleteJob.getQueryResults();
        console.log(`Existing Per_Key_Per_Day entries for Key ${Key} deleted.`);

        if (perKeyPerDayRows.length > 0) {
            await bigQueryClient
                .dataset(bigQueryDataset)
                .table(bigQueryTable2)
                .insert(perKeyPerDayRows);
            console.log(`New Per_Key_Per_Day entries for Key ${Key} inserted successfully.`);
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
