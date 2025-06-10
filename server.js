const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path'); // Path is no longer strictly needed for BigQuery init, but kept as it might be used elsewhere
const dotenv = require('dotenv');

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
// const keyFilePath = path.join(__dirname, process.env.GOOGLE_KEY_FILE); // This line is no longer needed and should be removed or commented out
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE;
const bigQueryTable2 = "Per_Key_Per_Day";
const bigQueryTable3 = "Per_Person_Per_Day";


const app = express();

// Middleware setup
app.use(cors());
app.use(express.json());

// Security headers to enable COOP and COEP
app.use((req, res, next) => {
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
  next();
});

// --- IMPORTANT CHANGE HERE: BigQuery client initialization using environment variables ---

// --- ADDED THESE LINES FOR DEBUGGING ---
console.log('DEBUG: GOOGLE_PROJECT_ID:', process.env.GOOGLE_PROJECT_ID);
console.log('DEBUG: BIGQUERY_CLIENT_EMAIL:', process.env.BIGQUERY_CLIENT_EMAIL);
console.log('DEBUG: BIGQUERY_PRIVATE_KEY exists:', !!process.env.BIGQUERY_PRIVATE_KEY); // Checks if it's truthy
if (process.env.BIGQUERY_PRIVATE_KEY) {
  console.log('DEBUG: First 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.substring(0, 50));
  console.log('DEBUG: Last 50 chars of private key:', process.env.BIGQUERY_PRIVATE_KEY.slice(-50));
  console.log('DEBUG: Private key contains \\n:', process.env.BIGQUERY_PRIVATE_KEY.includes('\\n'));
}
// --- END DEBUGGING LINES ---

const bigQueryClient = new BigQuery({
  projectId: process.env.GOOGLE_PROJECT_ID,
  credentials: {
    client_email: process.env.BIGQUERY_CLIENT_EMAIL,
    // The private key from env var might have escaped newlines, so replace them
    private_key: process.env.BIGQUERY_PRIVATE_KEY?.replace(/\\n/g, '\n'),
  },
  scopes: ['https://www.googleapis.com/auth/bigquery'], // More appropriate scope for BigQuery operations
});
// --- END IMPORTANT CHANGE ---


// Duplicate middleware setup, keeping only one for clarity
// app.use(cors()); // Already declared above
// app.use(express.json()); // Already declared above


app.get('/api/data', async (req, res) => {
  try {
    // Get limit, offset, and email from query parameters
    const limit = parseInt(req.query.limit, 500) || 500; // default to 10 rows
    const offset = parseInt(req.query.offset, 500) || 0; // default to start at the beginning
    const email = req.query.email; // email from query parameters

    // Ensure email is provided
    if (!email) {
      return res.status(400).json({ message: 'Email is required' });
    }
    const query = `
      SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` 
WHERE 
    REGEXP_CONTAINS(Email, CONCAT('(^|[[:space:],])', @email, '([[:space:],]|$)')) 
    AND (
        (Planned_Start_Timestamp IS NULL AND Planned_Delivery_Timestamp IS NULL) 
        OR 
        (Step_ID = 0 AND Planned_Start_Timestamp IS NOT NULL AND Planned_Delivery_Timestamp IS NOT NULL)
        OR 
        (Step_ID = 0 AND Planned_Start_Timestamp IS NOT NULL AND Planned_Delivery_Timestamp IS NULL)
        OR 
        (Step_ID = 0 AND Planned_Start_Timestamp IS NULL AND Planned_Delivery_Timestamp IS NOT NULL)
    )
ORDER BY DelCode_w_o__ 
LIMIT @limit OFFSET @offset;

        `;
    const options = {
      
      query: query,
      params: { limit, offset, email },
    };

    const [rows] = await bigQueryClient.query(options);
    console.log('Data fetched from BigQuery:', rows);

    // Group the data by DelCode_w_o__
    const groupedData = rows.reduce((acc, item) => {
      const key = item.DelCode_w_o__;
      if (!acc[key]) {
        acc[key] = [];
      }
      acc[key].push(item);
      return acc;
    }, {});

    res.status(200).json(groupedData);
  } catch (err) {
    console.error('Error querying BigQuery:', err.message, err.stack);
    res.status(500).json({ message: err.message, stack: err.stack });
  }
});

// The commented out app.get('/api/data') block below is preserved for your reference if you need to uncomment it.
// app.get('/api/data', async (req, res) => {
//    try {
//      // Get limit, offset, and email from query parameters
//      const limit = parseInt(req.query.limit, 500) || 500; // default to 500 rows
//      const offset = parseInt(req.query.offset, 500) || 0; // default to start at the beginning
//      const email = req.query.email; // email from query parameters

//      // Ensure email is provided
//      if (!email) {
//        return res.status(400).json({ message: 'Email is required' });
//      }

//      // Define additional emails for special user
//      const additionalEmails = [
//        'archana.l@brightbraintech.com',
//        'sarthak.c@brightbraintech.com'
//      ];

//      // Build the query conditionally based on the email
//      let emailCondition;
//      if (email === 'meghna.j@brightbrain.com') {
//        // Include tasks for the additional emails if email matches meghna.j
//        const emailList = [`'${email}'`, ...additionalEmails.map(e => `'${e}'`)].join(',');
//        emailCondition = `Email IN (${emailList})`;
//      } else {
//        // Regular email filtering
//        emailCondition = `REGEXP_CONTAINS(Email, CONCAT('(^|[[:space:],])', @email, '([[:space:],]|$)'))`;
//      }

//      const query = `
//        SELECT * //        FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` 
//        WHERE 
//            ${emailCondition} 
//            AND (
//                (Planned_Start_Timestamp IS NULL AND Planned_Delivery_Timestamp IS NULL) 
//                OR 
//                (Step_ID = 0 AND Planned_Start_Timestamp IS NOT NULL AND Planned_Delivery_Timestamp IS NOT NULL)
//                OR 
//                (Step_ID = 0 AND Planned_Start_Timestamp IS NOT NULL AND Planned_Delivery_Timestamp IS NULL)
//                OR 
//                (Step_ID = 0 AND Planned_Start_Timestamp IS NULL AND Planned_Delivery_Timestamp IS NOT NULL)
//            )
//        ORDER BY DelCode_w_o__ 
//        LIMIT @limit OFFSET @offset;
//      `;

//      const options = {
//        query: query,
//        params: { limit, offset, email },
//      };

//      const [rows] = await bigQueryClient.query(options);
//      console.log('Data fetched from BigQuery:', rows);

//      // Group the data by DelCode_w_o__
//      const groupedData = rows.reduce((acc, item) => {
//        const key = item.DelCode_w_o__;
//        if (!acc[key]) {
//          acc[key] = [];
//        }
//        acc[key].push(item);
//        return acc;
//      }, {});

//      res.status(200).json(groupedData);
//    } catch (err) {
//      console.error('Error querying BigQuery:', err.message, err.stack);
//      res.status(500).json({ message: err.message, stack: err.stack });
//    }
// });



// Route to get data from the Per_Key_Per_Day table with summed Duration
app.get('/api/per-key-per-day', async (req, res) => {
  try {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
    const [rows] = await bigQueryClient.query(query);

    // Group the data by Key and sum the Duration
    const groupedData = rows.reduce((acc, item) => {
      const key = item.Key;
      if (!acc[key]) {
        acc[key] = { totalDuration: 0, entries: [] };
      }
      acc[key].totalDuration += parseFloat(item.Duration) || 0; // Add Duration
      acc[key].entries.push(item); // Add the item itself for reference if needed
      return acc;
    }, {});

    console.log("Grouped data with total duration:", groupedData);
    res.status(200).json(groupedData);
  } catch (err) {
    console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
    res.status(500).json({ message: err.message, stack: err.stack });
  }
});

// Route to get data from the Per_Person_Per_Day table with summed Duration
app.get('/api/per-person-per-day', async (req, res) => {
  try {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\``;
    const [rows] = await bigQueryClient.query(query);

    // Directly return the fetched rows
    console.log("Fetched data:", rows);
    res.status(200).json(rows);
  } catch (err) {
    console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
    res.status(500).json({ message: err.message, stack: err.stack });
  }
});







// post req
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
    Planned_Delivery_Timestamp,
    Responsibility,
    Current_Status,
    Email,
    Total_Tasks,
    Completed_Tasks,
    Planned_Tasks,
    Percent_Tasks_Completed,
    Created_at,
    Updated_at,
    Time_Left_For_Next_Task_dd_hh_mm_ss,
    Card_Corner_Status,
    sliders // This is expected to be an array of slider data
  } = req.body;

  console.log("Hi",req.body);

  // Check if sliders data is provided
  if (!sliders || sliders.length === 0) {
    return res.status(400).send({ error: 'Slider data is mandatory.' });
  }

  try {
    // Handle task details (insert or update)
    const checkQuery = `SELECT Key FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` WHERE Key = @Key`;
    const checkOptions = {
      query: checkQuery,
      params: { Key },
      types: { Key: 'INT64' }
    };

    const [existingTasks] = await bigQueryClient.query(checkOptions);

    if (existingTasks.length > 0) {
      // If task exists, update it
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
        Email =  @Email,
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
      // If task doesn't exist, insert it
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

    // Handle slider values (insert into Per_Key_Per_Day)
    console.log('Received sliders data:', sliders);

    // Prepare the insert or update queries for sliders
    const insertOrUpdateSliderQueries = await Promise.all(sliders.map(async (slider) => {
      const selectQuery = {
        query: `SELECT duration FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` WHERE Key = @Key AND day = @day AND Planned_Delivery_Slot=@Planned_Delivery_Slot LIMIT 1`,
        params: {
          Key: Number(Key),
          day: slider.day,
          Planned_Delivery_Slot:slider.slot,
        },
        types: {
          Key: 'INT64',
          day: 'STRING',
          Planned_Delivery_Slot: 'STRING',
        },
      };

      // Check if the slider data already exists
      const [sliderRows] = await bigQueryClient.query(selectQuery);

      if (sliderRows.length > 0) {
        // Update existing slider record
        return {
          query: `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` SET duration = @duration WHERE Key = @Key AND day = @day AND Planned_Delivery_Slot=@Planned_Delivery_Slot`,
          params: {
            Key: Number(Key),
            day: slider.day,
            duration: Number(slider.duration),
            Planned_Delivery_Slot:slider.slot,
          },
          types: {
            Key: 'INT64',
            day: 'STRING',
            duration: 'INT64',
            Planned_Delivery_Slot:'STRING',
          },
        };
      } else {
        // Insert new slider record
        return {
          query: `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` (Key, day, duration,Planned_Delivery_Slot) VALUES (@Key, @day, @duration,@Planned_Delivery_Slot)`,
          params: {
            Key: Number(Key),
            day: slider.day,
            duration: Number(slider.duration),
            Planned_Delivery_Slot:slider.slot
          },
          types: {
            Key: 'INT64',
            day: 'STRING',
            duration: 'INT64',
            Planned_Delivery_Slot:'STRING',
          },
        };
      }
    }));

    // Execute each query (either an INSERT or UPDATE) based on the result
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
  console.log("hi",req.params)
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
