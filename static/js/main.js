$(document).ready(function() {
    const timeInterval = 30;
  $('#driving-data-table').DataTable({
      serverSide: true,
      processing: true,
      responsive: true,
      "ordering": false,
      ajax: {
          url: '/fetch_data',
          type: 'POST',
      },
      columns: [
          { data: 0 },
          { data: 1 },
          { data: 2 },
          { data: 3 },
          { data: 4 },
      ],
      pageLength: 25,
      language: {
          paginate: {
              previous: '&laquo;',
              next: '&raquo;',
          },
      },
      ordering: false,
      dom: "<'flex flex-wrap items-center justify-between'<'flex-grow'l><'flex-grow'f>>" +
           "<'w-full overflow-x-auto'tr>" +
           "<'flex flex-wrap items-center justify-between'<'flex-grow'i><'flex-grow'p>>",
  });

  // Tailwind CSS pagination styling
  $('#driving-data-table').on('draw.dt', function() {
      $('.paginate_button').addClass('px-3 py-1 mx-1 text-sm text-gray-700 bg-white border border-gray-300 rounded-md focus:outline-none focus:ring focus:ring-indigo-200 focus:border-indigo-300');
      $('.paginate_button.current').addClass('text-white bg-indigo-600');
  });

  const socket = io();
  let drivingSpeedChart = null;

    function formatDateToISOString(date) {
        return date.toISOString().slice(0, 19).replace("T", " ");
    }
    
    // Initialize Chart.js
    function createChart(data) {
        console.log("Data passed to createChart:", data);
        // Replace the canvas element
        const oldCanvas = document.getElementById("drivingSpeedChart");
        const newCanvas = document.createElement("canvas");
        newCanvas.id = "drivingSpeedChart";
        oldCanvas.parentNode.replaceChild(newCanvas, oldCanvas);
    
        // Get the new canvas context
        const ctx = document.getElementById("drivingSpeedChart").getContext("2d");
    
        if (drivingSpeedChart !== null) {
            drivingSpeedChart.destroy();
        }
    
        const chartData = {
            labels: data.map((driver) => driver.car_plate_number),
            datasets: [
                {
                    label: "Driving Speed",
                    data: data.map((driver) => driver.speed),
                    backgroundColor: "rgba(75, 192, 192, 0.2)",
                    borderColor: "rgba(75, 192, 192, 1)",
                    borderWidth: 1,
                },
            ],
        };
    
        drivingSpeedChart = new Chart(ctx, {
            type: "bar",
            data: chartData,
            options: {
                scales: {
                    y: {
                        beginAtZero: true,
                        suggestedMax: 120,
                    },
                },
            },
        });
    }
    
// Update chart with new data
function update_chart(data) {
    console.log("Received data:", data);
    // Group data by car_plate_number
    const groupedData = data.reduce((accumulator, currentValue) => {
        const key = currentValue.car_plate_number;
        if (!accumulator[key]) {
            accumulator[key] = {
                car_plate_number: key,
                speed: 0,
                count: 0
            };
        }
        accumulator[key].speed += currentValue.Speed; // Change this line
        accumulator[key].count += 1;

        return accumulator;
    }, {});

    // Calculate average speed for each car_plate_number
    const averageSpeedData = Object.values(groupedData).map((item) => {
        item.speed = item.speed / item.count;
        return item;
    });

    if (averageSpeedData.length === 0) {
        return;
    }
    check_for_warnings(data);
    createChart(averageSpeedData);
}
    
  // Check for speeding drivers and display warnings
    function check_for_warnings(data) {
        const speeding_drivers = data.filter((driver) => driver.isOverspeed === 1);

        // Clear existing warnings
        const warningDiv = document.getElementById("warnings");
        warningDiv.innerHTML = '';

        const speedingCounter = {};

        if (speeding_drivers.length) {
            speeding_drivers.forEach((driver) => {
                const licensePlate = driver.car_plate_number;
                const speed = driver.Speed;     

                if (!speedingCounter[licensePlate]) {
                    speedingCounter[licensePlate] = { count: 0, speed: speed };
                }
                speedingCounter[licensePlate].count++;
            });

            // Display warnings with the counter
            for (const licensePlate in speedingCounter) {
                const count = speedingCounter[licensePlate].count;
                const speed = speedingCounter[licensePlate].speed;
                warningDiv.innerHTML += `<p>Warning: Driver ${licensePlate} is speeding (${count} times) (${speed} km/h)</p>`;
            }
        }
    }

    datePicker.addEventListener('change', (e) => {
        const selectedDate = e.target.value;
        console.log("Selected date:", selectedDate);
        setEarliestTime(selectedDate);

        const start_time = getInitialTimeOffset();
        const time_interval = 30;

        console.log("Emitting selected_date_and_time:", {
            selected_date: datePicker.value,
            time_offset: start_time,
            time_interval: time_interval
        });

    // Emit the selected date and time
    socket.emit("selected_date_and_time", datePicker.value, start_time, time_interval);
    });

    // Listen for chart updates from the server
    socket.on('connect', () => {

        // Emit the initially selected date
        const start_time = getInitialTimeOffset();
        const time_interval = 30;
        socket.emit("selected_date_and_time", datePicker.value, start_time, time_interval);

        // Listen for chart updates from the server
        socket.on("update_chart", (data) => {
            console.log("Received data from server:", data);
            if (!Array.isArray(data)) {
                data = [data];
            }
            console.log("data.length:", data.length);
            console.log("data[0]:", data[0]);
        
            if (data.length > 0 && data[0].Time) {
                console.log("data[0].Time:", data[0].Time); 
                const actualStartTime = new Date(data[0].Time); 
                console.log("actualStartTime:", actualStartTime);
                updateCurrentTimestampRangeDisplay(actualStartTime);
            } else {
                console.log("Skipped timestamp update due to missing data or start_time");
            }
            update_chart(data);
        });
        setInterval(updateTimerDisplay, 1000);
    });

    // Listen for warnings updates from the server
    socket.on('update_warnings', (warnings) => {
        const warningDiv = document.getElementById("warnings");
        warningDiv.innerHTML = "";
        warnings.forEach((warning) => {
            warningDiv.innerHTML += `<p>Warning: Driver ${warning.car_plate_number} is speeding (${warning.overspeed_count} times)</p>`;
        });
    });

    function setEarliestTime(selected_date, updateSelectedDateEarliestTime = true) {
        $.ajax({
            url: "/get_earliest_timestamp",
            type: "GET",
            data: { specific_date: selected_date },
            success: function (response) {
                if (response.timestamp) {
                    sessionStorage.setItem("earliest_time", response.timestamp);
                    sessionStorage.setItem("initial_date", new Date(selected_date).toISOString());

                    if (updateSelectedDateEarliestTime) {
                        sessionStorage.setItem("selected_date_earliest_time", response.timestamp);
                    }

                    updateChartAndWarnings();
                }
            },
        });
    }
    
    function updateChartAndWarnings() {
        
        const earliest_time = sessionStorage.getItem("earliest_time");
        const initial_date = new Date(sessionStorage.getItem("initial_date"));
    
        if (earliest_time) {
            const initial_start_time = new Date(earliest_time);
            const end_time = new Date(initial_start_time.getTime() + 30000);

            console.log('Before updating sessionStorage:', formatDateToISOString(end_time));
            sessionStorage.setItem("earliest_time", formatDateToISOString(end_time));
            console.log('After updating sessionStorage:', sessionStorage.getItem("earliest_time"));
    
            const datePickerValue = document.getElementById('datePicker').value;
            const selectedDate = new Date(datePickerValue);
            if (initial_date.toISOString().slice(0, 10) === selectedDate.toISOString().slice(0, 10)) { 
                // Emit the selected date, start time, and time_interval (30 seconds)
                console.log('Before emitting selected_date_and_time:', datePicker.value, initial_start_time.toISOString().slice(0, 19).replace("T", " "), 30);
                socket.emit("selected_date_and_time", selectedDate, initial_start_time.toISOString().slice(0, 19).replace("T", " "), 30);
            } else {
                setEarliestTime(datePickerValue); 
            }
        }
    }
        
    function updateTimerDisplay() {
        const timer = document.getElementById("timer");
        let timeLeft = parseInt(timer.textContent);
        timeLeft--;
    
        if (timeLeft <= 0) {
            timeLeft = 30;
    
            // Retrieve the current "selected_date_earliest_time" value
            const selected_date_earliest_time = sessionStorage.getItem("selected_date_earliest_time");
            if (selected_date_earliest_time) {
                // Increment the start time by 30 seconds
                const start_time = new Date(selected_date_earliest_time);
                start_time.setSeconds(start_time.getSeconds() + 30);
    
                // Update the "selected_date_earliest_time" value in sessionStorage
                sessionStorage.setItem("selected_date_earliest_time", start_time.toISOString());
    
                // Emit the new start time
                socket.emit(
                    "selected_date_and_time",
                    datePicker.value,
                    start_time.toISOString().slice(0, 19).replace("T", " "),
                    30
                );
            }
        }
        timer.textContent = timeLeft;
    }

    function updateCurrentTimestampRangeDisplay(startTime) {
        const endTime = new Date(startTime.getTime() + 30000); // Add 30 seconds
        const currentTimestampRange = document.getElementById("currentTimestampRange");
        currentTimestampRange.textContent = `${startTime.toLocaleTimeString()} - ${endTime.toLocaleTimeString()}`;
    }

    function getInitialTimeOffset() {
        const earliest_time = sessionStorage.getItem("earliest_time");
        const selected_date_earliest_time = sessionStorage.getItem("selected_date_earliest_time");
        if (earliest_time && selected_date_earliest_time) {
            const start_time = new Date(earliest_time);
            const selected_time = new Date(selected_date_earliest_time);
    
            return selected_time.toISOString().slice(0, 19).replace("T", " ");
        }
        return "";
    }
    
});
