import React, { useState, useEffect, memo, useCallback } from 'react';
import { Form, Input, Button, Slider, DatePicker, Select, notification, Row, Col } from 'antd';
import moment from 'moment';
import './FormComponent.css';

const { Option } = Select;

// Define the emails of users who can see and edit the full list
const ADMIN_EMAILS = [
    "neelam.p@brightbraintech.com",
    "meghna.j@brightbraintech.com",
    "zoya.a@brightbraintech.com",
    "shweta.g@brightbraintech.com",
    "hitesh.r@brightbraintech.com"
];

// Removed hardcoded PERSON_EMAIL_DATA_MAP and ALL_AVAILABLE_PERSONS_HARDCODED
// This data will now be fetched dynamically from the backend.

// Define the base URL for your backend API
const BACKEND_API_BASE_URL = process.env.REACT_APP_BACKEND_URL || 'http://localhost:3001';
console.log('Using Backend API URL:', BACKEND_API_BASE_URL);


const FormComponent = ({ onSubmit, task, currentUserEmail }) => {
    const [form] = Form.useForm();
    const [sliderCount, setSliderCount] = useState(0);
    const [hours, setHours] = useState({});
    const [startDate, setStartDate] = useState(null);
    const [endDate, setEndDate] = useState(null);

    const [personResponsible, setPersonResponsible] = useState('');
    const [numberOfDays, setNumberOfDays] = useState(0);
    const [existingSchedules, setExistingSchedules] = useState({});

    // New state to hold dynamically fetched person data
    const [fetchedPersonsData, setFetchedPersonsData] = useState({
        personEmailMap: {},
        allAvailablePersons: []
    });
    const [loadingPersons, setLoadingPersons] = useState(true);


    console.log('FormComponent: currentUserEmail received:', currentUserEmail);
    const isAdmin = ADMIN_EMAILS.includes(currentUserEmail);
    console.log('FormComponent: isAdmin calculated as:', isAdmin);


    // Memoize the mapping logic to prevent unnecessary re-renders
    const getPersonNameFromEmail = useCallback((email) => {
        // Use the dynamically fetched personEmailMap
        const entry = Object.entries(fetchedPersonsData.personEmailMap).find(([, value]) => value.primaryEmail === email || value.allEmails.includes(email));
        return entry ? entry[0] : null;
    }, [fetchedPersonsData.personEmailMap]); // Dependency on the fetched map


    // --- EFFECT HOOK 1: FETCH TASK DATA AND INITIALIZE FORM FIELDS ---
    useEffect(() => {
        const fetchTaskAndScheduleData = async () => {
            try {
                if (task) {
                    form.setFieldsValue({
                        name: task.Task_Details || '',
                    });

                    // Set initial start and end dates from task if available
                    // Ensure dates are parsed as UTC to avoid timezone issues
                    // Crucially, ensure startDate is a UTC moment object from the start
                    const initialStartDate = task.Planned_Start_Timestamp ? moment.utc(task.Planned_Start_Timestamp).startOf('day') : null;
                    const initialEndDate = task.Planned_Delivery_Timestamp ? moment.utc(task.Planned_Delivery_Timestamp).startOf('day') : null;

                    setStartDate(initialStartDate);
                    setEndDate(initialEndDate);

                    if (initialStartDate && initialEndDate) {
                        // Calculate daysDiff based on startOf('day') for both
                        const daysDiff = initialEndDate.diff(initialStartDate, 'days') + 1;
                        setNumberOfDays(daysDiff);
                        setSliderCount(daysDiff);
                    } else {
                        setNumberOfDays(0);
                        setSliderCount(0);
                    }

                    // Fetch per-key-per-day data
                    const response = await fetch(`${BACKEND_API_BASE_URL}/api/per-key-per-day`);
                    if (!response.ok) {
                        const errorText = await response.text();
                        throw new Error(`HTTP error! status: ${response.status}, message: ${errorText}`);
                    }
                    const data = await response.json();

                    const taskData = data[task.Key];
                    if (taskData) {
                        const taskEntries = taskData.entries;
                        const initialHours = {};

                        if (taskEntries && taskEntries.length > 0 && initialStartDate) {
                            taskEntries.forEach((entry) => {
                                if (entry.Duration !== undefined && entry.Day !== undefined) {
                                    const dayMoment = moment.utc(entry.Day.value); // Parse as UTC
                                    if (dayMoment.isValid() && dayMoment.isSameOrAfter(initialStartDate, 'day')) {
                                        const dayIndex = dayMoment.diff(initialStartDate, 'days');
                                        initialHours[dayIndex] = entry.Duration;
                                    }
                                }
                            });
                        }
                        setHours(initialHours);
                    }

                    // Fetch per-person-per-day data
                    const perPersonResponse = await fetch(`${BACKEND_API_BASE_URL}/api/per-person-per-day`);
                    if (!perPersonResponse.ok) {
                        const errorText = await perPersonResponse.text();
                        throw new Error(`HTTP error! status: ${perPersonResponse.status}, message: ${errorText}`);
                    }
                    const perPersonData = await perPersonResponse.json();

                    const schedules = {};
                    perPersonData.forEach((entry) => {
                        const { Responsibility, Day, Duration_In_Minutes } = entry;
                        const date = Day.value;
                        if (!schedules[Responsibility]) {
                            schedules[Responsibility] = {};
                        }
                        schedules[Responsibility][date] = Duration_In_Minutes;
                    });
                    setExistingSchedules(schedules);
                }
            } catch (error) {
                console.error("Error fetching task data or schedules:", error);
                notification.error({
                    message: 'Error',
                    description: `Failed to load task data or existing schedules: ${error.message}. Please check network and server logs.`,
                });
            } finally {
                // Ensure loading is set to false even if there's an error
                // This might be redundant with the outer loading state, but good for clarity
            }
        };

        fetchTaskAndScheduleData();
    }, [task, form]); // Dependencies ensure this runs when task or form changes


    // --- NEW EFFECT HOOK: FETCH PEOPLE MAPPING DATA ---
    useEffect(() => {
        const fetchPeopleMapping = async () => {
            setLoadingPersons(true);
            try {
                // Assuming your backend exposes this data at /api/people-mapping
                const response = await fetch(`${BACKEND_API_BASE_URL}/api/people-mapping`);
                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`HTTP error! status: ${response.status}, message: ${errorText}`);
                }
                const data = await response.json();

                const newPersonEmailMap = {};
                const newAllAvailablePersons = [];
                data.forEach(entry => {
                    if (entry.Current_Employes && entry.Emp_Emails) {
                        newPersonEmailMap[entry.Current_Employes] = {
                            primaryEmail: entry.Emp_Emails,
                            allEmails: entry.Emp_Emails // Assuming single email for now, adjust if multiple comma-separated emails
                        };
                        newAllAvailablePersons.push(entry.Current_Employes);
                    }
                });
                setFetchedPersonsData({
                    personEmailMap: newPersonEmailMap,
                    allAvailablePersons: newAllAvailablePersons
                });
            } catch (error) {
                console.error("Error fetching people mapping:", error);
                notification.error({
                    message: 'Error',
                    description: `Failed to load person data: ${error.message}. Please ensure the backend endpoint /api/people-mapping is correctly configured.`,
                });
            } finally {
                setLoadingPersons(false);
            }
        };
        fetchPeopleMapping();
    }, []); // Empty dependency array to run once on mount

    // --- EFFECT HOOK 2: SET INITIAL PERSON RESPONSIBLE AND CONTROL EDITABILITY ---
    useEffect(() => {
        // This effect should ideally run AFTER fetchedPersonsData is available.
        // Adding fetchedPersonsData.allAvailablePersons to dependencies ensures this.
        if (loadingPersons) return; // Wait until persons data is loaded

        const initialResponsibilityFromTask = task?.Responsibility || '';
        const userPersonName = getPersonNameFromEmail(currentUserEmail);

        if (isAdmin) {
            // Admin user: Can see full list, try to pre-fill from task.
            if (initialResponsibilityFromTask && fetchedPersonsData.allAvailablePersons.includes(initialResponsibilityFromTask)) {
                setPersonResponsible(initialResponsibilityFromTask);
                form.setFieldsValue({ personResponsible: initialResponsibilityFromTask });
            } else {
                setPersonResponsible('');
                form.setFieldsValue({ personResponsible: undefined });
            }
        } else {
            // Non-admin user: Only allowed to see their mapped name.
            if (userPersonName && fetchedPersonsData.allAvailablePersons.includes(userPersonName)) {
                setPersonResponsible(userPersonName);
                form.setFieldsValue({ personResponsible: userPersonName });
            } else {
                // If current user's email doesn't map to a known person, or that person
                // isn't in the hardcoded list, set to empty/undefined and disable.
                setPersonResponsible('');
                form.setFieldsValue({ personResponsible: undefined });
            }
        }
    }, [task, currentUserEmail, form, getPersonNameFromEmail, isAdmin, loadingPersons, fetchedPersonsData.allAvailablePersons]);


    const handleStartDateChange = (date) => {
        // Ensure the date set to state is a UTC moment object if it's not null
        const newStartDate = date ? moment.utc(date).startOf('day') : null;
        setStartDate(newStartDate);
        // Recalculate end date and slider count immediately
        if (newStartDate && numberOfDays > 0) {
            calculateEndDate(newStartDate, numberOfDays);
        } else {
            setEndDate(null);
            setSliderCount(0);
            setHours({}); // Clear hours when startDate or numberOfDays is invalid
        }
    };


    const handleNumberOfDaysChange = (e) => {
        const days = e.target.value;
        const numericDays = parseInt(days, 10) || 0;
        setNumberOfDays(numericDays);
        // Recalculate end date and slider count immediately
        if (startDate && numericDays > 0) {
            calculateEndDate(startDate, numericDays);
        } else {
            setEndDate(null);
            setSliderCount(0);
            setHours({}); // Clear hours when startDate or numberOfDays is invalid
        }
    };

    const calculateEndDate = useCallback((start, days) => {
        if (start && start.isValid() && days > 0) {
            // End date is 'days' inclusive, so add days - 1
            const calculatedEndDate = start.clone().add(days - 1, 'days').startOf('day');
            setEndDate(calculatedEndDate);
            setSliderCount(days);
        } else {
            setEndDate(null);
            setSliderCount(0);
        }
    }, []); // No dependencies needed for useCallback as start and days are passed as args


    const calculateTotalTime = () => {
        return Object.values(hours).reduce((acc, curr) => {
            return acc + (typeof curr === 'number' ? curr : 0);
        }, 0);
    };


    const handleSubmit = () => {
        form
            .validateFields()
            .then((values) => {
                // Ensure plannedStartTimestamp and plannedDeliveryTimestamp are formatted as DATE strings (YYYY-MM-DD)
                // for BigQuery DATE type, or TIMESTAMP with UTC for TIMESTAMP type.
                // Based on previous discussions, Planned_Start_Timestamp and Planned_Delivery_Timestamp are TIMESTAMP.
                const plannedStartTimestamp = startDate
                    ? moment(startDate).startOf('day').utc().format("YYYY-MM-DD HH:mm:ss.SSSSSS") + " UTC"
                    : null;

                const plannedDeliveryTimestamp = endDate
                    ? moment(endDate).startOf('day').utc().format("YYYY-MM-DD HH:mm:ss.SSSSSS") + " UTC"
                    : null;

                const totalTime = calculateTotalTime();
                const perKeyPerDayRows = Array.from({ length: sliderCount }).map((_, index) => {
                    // Ensure calculatedDay is based on the UTC-parsed startDate and formatted for BigQuery DATE
                    const calculatedDay = moment.utc(startDate).add(index, 'days'); // startDate is already UTC
                    const formattedDay = calculatedDay.isValid() ? calculatedDay.format('YYYY-MM-DD') : null;
                    return {
                        Key: task.Key, // Include the task Key for each entry
                        Day: formattedDay,
                        Duration: hours[index] || 0, // Use the value from the hours state
                        Duration_Unit: "min", // Set to "min" as requested
                        Planned_Delivery_Slot: "Null", // Assuming 'Null' is the default slot
                        Responsibility: personResponsible, // Use the selected personResponsible
                    };
                });

                // Use the dynamically fetched person email data
                const selectedPersonEmailData = fetchedPersonsData.personEmailMap[personResponsible];
                const newEmail = selectedPersonEmailData ? selectedPersonEmailData.primaryEmail : null;
                const newEmails = selectedPersonEmailData ? selectedPersonEmailData.allEmails : null;
                
                const mainTaskData = {
                    Key: task.Key,
                    Delivery_code: task.Delivery_code,
                    DelCode_w_o__: task.DelCode_w_o__,
                    Step_ID: task.Step_ID,
                    Task_Details: values.name,
                    Frequency___Timeline: task.Frequency___Timeline,
                    Client: task.Client,
                    Short_Description: task.Short_Description,
                    Planned_Start_Timestamp: plannedStartTimestamp,
                    Planned_Delivery_Timestamp: plannedDeliveryTimestamp,
                    Responsibility: personResponsible, // This comes from the dropdown
                    Current_Status: task.Current_Status || 'Scheduled', // Default to 'Scheduled' if unassigned
                    Email: newEmail, // Use the dynamically determined email
                    Emails: newEmails, // Use the dynamically determined emails string
                    Total_Tasks: task.Total_Tasks,
                    Completed_Tasks: task.Completed_Tasks,
                    Planned_Tasks: task.Planned_Tasks,
                    Percent_Tasks_Completed: task.Percent_Tasks_Completed,
                    Created_at: task.Created_at || moment.utc().format("YYYY-MM-DD HH:mm:ss.SSSSSS") + " UTC", // Preserve original or set new
                    Updated_at: moment.utc().format("YYYY-MM-DD HH:mm:ss.SSSSSS") + " UTC", // Always update Updated_at
                    Time_Left_For_Next_Task_dd_hh_mm_ss: task.Time_Left_For_Next_Task_dd_hh_mm_ss,
                    Card_Corner_Status: task.Card_Corner_Status,
                };

                console.log('Main Task Data for submission:', mainTaskData);
                console.log('Per Key Per Day Rows for submission:', perKeyPerDayRows);

                // Send both mainTask and perKeyPerDayRows in the request body
                fetch(`${BACKEND_API_BASE_URL}/api/post`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        mainTask: mainTaskData,
                        perKeyPerDayRows: perKeyPerDayRows
                    }),
                })
                    .then((response) => {
                        if (!response.ok) {
                            return response.text().then(text => { throw new Error(text); });
                        }
                        return response.json();
                    })
                    .then(() => {
                        notification.success({
                            message: 'Task Updated',
                            description: 'Your task has been successfully updated!',
                        });
                        // Pass updated scheduling data back to parent (DeliveryDetail)
                        onSubmit({
                            personResponsible: mainTaskData.Responsibility,
                            totalTime: totalTime,
                            Planned_Delivery_Timestamp: mainTaskData.Planned_Delivery_Timestamp,
                            Current_Status: mainTaskData.Current_Status,
                            Email: mainTaskData.Email,
                            Emails: mainTaskData.Emails // Pass back new Emails as well
                        });
                    })
                    .catch((error) => {
                        notification.error({
                            message: 'Error',
                            description: error.message || 'An error occurred while updating the task.',
                        });
                    });
            })
            .catch((info) => {
                console.error('Validation Failed:', info);
                notification.error({
                    message: 'Error',
                    description: 'Please fill in all required fields correctly.',
                });
            });
    };


    const handleSliderChange = (index, value) => {
        // Ensure currentDay is based on the UTC-parsed startDate
        const currentDay = moment.utc(startDate).add(index, 'days').format('YYYY-MM-DD');
        const maxAllowedMinutes = 480;
        let effectiveValue = value;

        if (existingSchedules[personResponsible]?.[currentDay]) {
            const alreadyScheduledMinutes = existingSchedules[personResponsible][currentDay];
            const remainingMinutes = maxAllowedMinutes - (alreadyScheduledMinutes || 0);
            effectiveValue = Math.min(value, remainingMinutes);
            if (value > remainingMinutes) {
                notification.warning({
                    message: 'Time Limit Reached',
                    description: `Cannot schedule more than ${remainingMinutes} minutes for ${personResponsible} on ${currentDay} due to existing tasks.`,
                });
            }
        }

        setHours((prev) => ({ ...prev, [index]: effectiveValue }));
    };

    const handleInputChange = (index, value) => {
        let numericValue = parseInt(value, 10);
        if (isNaN(numericValue)) {
            numericValue = 0;
        }

        // Ensure currentDay is based on the UTC-parsed startDate
        const currentDay = moment.utc(startDate).add(index, 'days').format('YYYY-MM-DD');
        const maxAllowedMinutes = 480;
        let effectiveValue = numericValue;

        if (existingSchedules[personResponsible]?.[currentDay]) {
            const alreadyScheduledMinutes = existingSchedules[personResponsible][currentDay];
            const remainingMinutes = maxAllowedMinutes - (alreadyScheduledMinutes || 0);
            effectiveValue = Math.min(numericValue, remainingMinutes);
            if (numericValue > remainingMinutes) {
                notification.warning({
                    message: 'Time Limit Reached',
                    description: `Cannot schedule more than ${remainingMinutes} minutes for ${personResponsible} on ${currentDay} due to existing tasks.`,
                });
            }
        }

        setHours((prev) => ({
            ...prev,
            [index]: effectiveValue < 0 ? 0 : effectiveValue,
        }));
    };

    const customMarks = {
        1: '1 m',
        60: '1 h',
        120: '2 h',
        180: '3 h',
        240: '4 h',
        300: '5 h',
        360: '6 h',
        420: '7 h',
        480: '8 h',
    };

    // Function to disable dates outside the allowed range (past dates and beyond 2 months from today)
    const disabledDateRange = (current) => {
        // Can not select days before today
        const isPastDate = current && current.isBefore(moment().startOf('day'));
        // Can not select days more than 2 months from today (end of the month)
        const isFutureDateBeyondLimit = current && current.isAfter(moment().add(2, 'months').endOf('month'));
        return isPastDate || isFutureDateBeyondLimit;
    };

    // Define personsToDisplay based on user role and fetched data
    const personsToDisplay = isAdmin
        ? fetchedPersonsData.allAvailablePersons
        : (getPersonNameFromEmail(currentUserEmail) && fetchedPersonsData.allAvailablePersons.includes(getPersonNameFromEmail(currentUserEmail)))
            ? [getPersonNameFromEmail(currentUserEmail)]
            : [];

    console.log('FormComponent: personsToDisplay:', personsToDisplay); // Added console log here

    return (
        <Form form={form} layout="vertical">
            <Form.Item
                name="name"
                label="Task Name"
                rules={[{ required: true, message: 'Please input the task name!' }]}
            >
                <Input readOnly={true} />
            </Form.Item>

            <Row gutter={[8, 16]}>
                <Col xs={24} sm={8}>
                    <Form.Item label="Start Date">
                        <DatePicker
                            format="YYYY-MM-DD"
                            onChange={handleStartDateChange}
                            value={startDate}
                            placeholder="Select start date"
                            style={{ width: '100%' }}
                            disabledDate={disabledDateRange}
                        />
                    </Form.Item>
                </Col>
                <Col xs={24} sm={8}>
                    <Form.Item label="Number of Days">
                        <Input
                            type="number"
                            value={numberOfDays}
                            onChange={handleNumberOfDaysChange}
                            min={0}
                            style={{ width: '100%' }}
                        />
                    </Form.Item>
                </Col>
                <Col xs={24} sm={8}>
                    <Form.Item label="End Date">
                        <DatePicker
                            format="YYYY-MM-DD"
                            value={endDate}
                            disabled
                            style={{ width: '100%' }}
                        />
                    </Form.Item>
                </Col>
            </Row>

            {Array.from({ length: sliderCount }).map((_, index) => (
                <Form.Item
                    key={index}
                    label={`Hours for ${startDate ? moment.utc(startDate).add(index, 'days').format('DD/MM/YYYY') : `Day ${index + 1}`}`}
                >
                    <Row gutter={20}>
                        <Col xs={20}>
                            <Slider
                                marks={customMarks}
                                min={0}
                                max={480}
                                step={1}
                                onChange={(value) => handleSliderChange(index, value)}
                                value={hours[index] || 0}
                                tooltip={{ formatter: (value) => `${value} minutes` }}
                            />
                        </Col>
                        <Col xs={4}>
                            <Input
                                type="number"
                                min={0}
                                max={480}
                                value={hours[index] || 0}
                                onChange={(e) => handleInputChange(index, e.target.value)}
                                addonAfter="min"
                            />
                        </Col>
                    </Row>
                </Form.Item>
            ))}

            <Form.Item
                label="Person Responsible"
                name="personResponsible"
                rules={[{ required: true, message: 'Please select the person responsible!' }]}
            >
                <Select
                    placeholder="Select a person"
                    onChange={setPersonResponsible}
                    value={personResponsible || null} // Changed from || undefined to || null
                    showSearch
                    optionFilterProp={(input, option) =>
                        (option?.children ?? '').toLowerCase().includes(input.toLowerCase())
                    }
                    // Disable if the user is not an admin
                    disabled={!isAdmin || loadingPersons} // Disable if loading persons or not admin
                    loading={loadingPersons} // Show loading spinner if data is being fetched
                >
                    {personsToDisplay.map((person) => (
                        <Option key={person} value={person}>
                            {person}
                        </Option>
                    ))}
                </Select>
            </Form.Item>

            <Form.Item>
                <Button type="primary" htmlType="submit" onClick={handleSubmit}>
                    Submit
                </Button>
            </Form.Item>
        </Form>
    );
};

export default memo(FormComponent);
