// In server.js, after other imports and app setup:

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
    projectId: projectId,
    credentials: {
        client_email: process.env.BIGQUERY_CLIENT_EMAIL,
        private_key: process.env.BIGQUERY_PRIVATE_KEY ? process.env.BIGQUERY_PRIVATE_KEY.replace(/\\n/g, '\n') : undefined,
    },
});

// Existing routes (add your new route before the listen call)

// New endpoint to fetch people mapping
app.get('/api/people-mapping', async (req, res) => {
    const query = `
        SELECT Current_Employes, Emp_Emails
        FROM \`${projectId}.${bigQueryDataset}.People_To_Email_Mapping\`
    `;

    try {
        const [rows] = await bigQueryClient.query(query);
        // Ensure the data is in the format expected by the frontend
        // For example, if Emp_Emails can be multiple, you might want to split them
        const formattedRows = rows.map(row => ({
            Current_Employes: row.Current_Employes,
            Emp_Emails: row.Emp_Emails // Assuming Emp_Emails is a single email string or comma-separated
        }));
        res.status(200).json(formattedRows);
    } catch (error) {
        console.error('Error fetching people mapping from BigQuery:', error);
        res.status(500).send({ error: 'Failed to fetch people mapping data.' });
    }
});

// Existing /api/data route (GET all tasks)
app.get('/api/data', async (req, res) => {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\``;
    try {
        const [rows] = await bigQueryClient.query(query);
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
        // Assuming timestamp is already in "YYYY-MM-DD HH:mm:ss.SSSSSS UTC" format from frontend
        // BigQuery TIMESTAMP type expects UTC, without the " UTC" suffix if passed as a string literal
        // For parameterized queries, it's often better to pass as a Date object or ISO string without timezone suffix
        // Let's ensure it's a valid ISO string for BigQuery's TIMESTAMP type
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
        Created_at: formatTimestamp(Created_at), // Ensure Created_at is also formatted
        Updated_at: formatTimestamp(Updated_at),
        Time_Left_For_Next_Task_dd_hh_mm_ss: Time_Left_For_Next_Task_dd_hh_mm_ss,
        Card_Corner_Status: Card_Corner_Status,
    };

    // Prepare data for Per_Key_Per_Day table (sliders data)
    const perKeyPerDayRows = sliders.map(slider => ({
        Key: Key,
        Day: slider.day, // YYYY-MM-DD
        Duration: slider.duration, // minutes
        Slot: slider.slot,
        Responsibility: slider.personResponsible, // Person Responsible for this specific day/duration
    }));

    // Start a transaction or use a sequence of operations
    try {
        // 1. Update/Insert into the main task table
        // Use a MERGE statement or check for existence and then UPDATE/INSERT
        // For simplicity, let's assume an UPDATE based on Key for now.
        // If Key is unique and always exists for updates, this is fine.
        // If it might be a new task, you'd need an INSERT or MERGE.
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
            location: 'US', // Specify your BigQuery dataset location
        };
        const [mainTaskJob] = await bigQueryClient.createQueryJob(updateMainTaskOptions);
        await mainTaskJob.getQueryResults();
        console.log(`Main task with Key ${Key} updated successfully.`);

        // 2. Delete existing entries for this Key in Per_Key_Per_Day table to avoid duplicates
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

        // 3. Insert new entries into Per_Key_Per_Day table
        if (perKeyPerDayRows.length > 0) {
            // BigQuery insertRows can take an array of rows
            await bigQueryClient
                .dataset(bigQueryDataset)
                .table(bigQueryTable2)
                .insert(perKeyPerDayRows);
            console.log(`New Per_Key_Per_Day entries for Key ${Key} inserted successfully.`);
        }

        // 4. Update or insert into Per_Person_Per_Day (aggregated data)
        // This part needs careful consideration. If you want to aggregate
        // the new slider data into Per_Person_Per_Day, you'll need a more complex
        // MERGE or series of DELETE/INSERT/UPDATE statements that sum durations
        // for each person per day.
        // For now, let's assume Per_Person_Per_Day is derived or updated separately
        // or that this POST only affects the main task and per-key-per-day.
        // If you need real-time aggregation here, it's a more advanced BigQuery SQL task.

        res.status(200).send({ message: 'Task and associated schedule data updated successfully.' });

    } catch (error) {
        console.error('Error updating task and schedule in BigQuery:', error);
        // Log BigQuery insert errors if available
        if (error.response && error.response.insertErrors) {
            console.error('BigQuery specific insert errors details:');
            error.response.insertErrors.forEach((insertError, index) => {
                console.error(`  Row ${index} had errors:`);
                insertError.errors.forEach(e => console.error(`    - Reason: ${e.reason}, Message: ${e.message}`));
                console.error('  Raw row that failed:', JSON.stringify(insertError.row, null, 2));
            });
        } else if (error.code && error.errors) { // General Google Cloud error format
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
