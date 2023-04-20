fetch('/correlation_matrix')
  .then(response => response.text())
  .then(data => {
    const lines = data.trim().split('\n');
    const headers = [
      "",
      "total_rapid_accelerations",
      "total_rapid_decelerations",
      "total_neutral_slide",
      "total_neutral_slide_duration",
      "total_overspeed",
      "total_overspeed_duration",
      "total_fatigue_driving",
      "total_hthrottle_stop",
      "total_oil_leak",
      "average_speed",
      "total_distance"
    ];
    const matrix = lines.map(line => line.split(/\s+/).filter(str => str !== '').map(Number));
    const tableBody = document.querySelector('#correlation-matrix tbody');

    tableBody.classList.add(
        'divide-y',
        'divide-gray-200',
        'text-left',
        'text-gray-700'
      );
      
      matrix.forEach((row, rowIndex) => {
        const tableRow = document.createElement('tr');
        tableRow.classList.add('hover:bg-gray-100');
      
        const emptyCell = document.createElement('td');
        emptyCell.classList.add('p-2');
        tableRow.appendChild(emptyCell);
      
        const headerData = document.createElement('td');
        headerData.classList.add('font-bold', 'p-2', 'bg-gray-200');
        headerData.textContent = headers[rowIndex];
        tableRow.appendChild(headerData);
      
        row.forEach((value) => {
          const tableData = document.createElement('td');
          tableData.classList.add('p-2');
          tableData.textContent = value.toFixed(6);
          tableRow.appendChild(tableData);
        });
      
        tableBody.appendChild(tableRow);
      });
  });
