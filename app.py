from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import csv
import os
import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from datetime import datetime, timedelta

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

spark = (
    SparkSession.builder.appName("Driving Behavior Analysis")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.cores", "2")  
    .config("spark.executor.instances", "4")  
    .config("spark.executor.memory", "2g")  
    .config("spark.driver.memory", "1g")  
    .config("spark.driver.cores", "1")  
    .config("spark.default.parallelism", "100")  
    .getOrCreate()
)

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DatetimeEncoder, self).default(obj)
    
def read_driving_data(folder_path):
    driving_data = {}
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)
                for row in reader:
                    car_plate_number = row[1]
                    try:
                        overspeed_count = int(row[14]) if row[14] else 0
                        fatigue_driving = int(row[16]) if row[16] else 0
                        overspeed_time = float(row[15]) if row[15] else 0.0
                        neutral_slide_time = float(row[12]) if row[12] else 0.0
                    except IndexError:
                        overspeed_count = 0
                        fatigue_driving = 0
                        overspeed_time = 0.0
                        neutral_slide_time = 0.0

                    if car_plate_number not in driving_data:
                        driving_data[car_plate_number] = {'overspeed_count': 0, 'fatigue_driving_count': 0, 'overspeed_time': 0, 'neutral_slide_time': 0}

                    driving_data[car_plate_number]['overspeed_count'] += overspeed_count
                    driving_data[car_plate_number]['fatigue_driving_count'] += fatigue_driving
                    driving_data[car_plate_number]['overspeed_time'] += overspeed_time
                    driving_data[car_plate_number]['neutral_slide_time'] += neutral_slide_time

    return [([key] + list(values.values())) for key, values in driving_data.items()]

def read_csv_data_to_dataframe(folder_path):
    data = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            data.append(spark.read.csv(file_path, header=True, inferSchema=True))

    combined_dataframe = data[0]
    for dataframe in data[1:]:
        combined_dataframe = combined_dataframe.union(dataframe)

    return combined_dataframe

data_folder_path = 'output_data'
combined_dataframe = read_csv_data_to_dataframe(data_folder_path)
combined_dataframe = combined_dataframe.fillna(0, subset=['isOverspeed'])

def struct_to_dict(struct):
    return struct.asDict()

def get_driving_speed_data(data_folder_path, start_time, time_interval):
    driving_speed_data = {}

    end_time = start_time + timedelta(seconds=time_interval)

    filtered_data = combined_dataframe.filter((F.col("Time") >= start_time) & (F.col("Time") < end_time))
    driving_speed_data = filtered_data.groupBy('carPlateNumber').agg(
        F.collect_list(F.struct(F.col("Time"), F.col("Speed").cast("float"), F.col("isOverspeed"))).alias("records"),
        F.mean("Speed").alias("avg_speed"),
        F.sum("isOverspeed").alias("overspeed_count")
    )

    driving_speed_data = driving_speed_data.withColumn(
        "records", F.expr("transform(records, x -> named_struct('Time', x.Time, 'Speed', x.col2, 'isOverspeed', x.isOverspeed))")
    )

    return driving_speed_data

def filter_data_by_date(dataframe, specific_date):
    return dataframe.filter(col('Time').startswith(specific_date))

def get_earliest_timestamp(dataframe, specific_date):
    filtered_data = filter_data_by_date(dataframe, specific_date)
    min_timestamp = filtered_data.agg({"Time": "min"}).collect()[0][0]

    if not min_timestamp:
        return None

    return min_timestamp

data_folder_path = 'output_data'
driving_data = read_driving_data(data_folder_path)

@app.route('/')
def driving_behavior():
    return render_template('driving_behavior.html', data=driving_data)

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    draw = int(request.form['draw'])
    start = int(request.form['start'])
    length = int(request.form['length'])
    search_value = request.form['search[value]']

    if search_value:
        filtered_data = [row for row in driving_data if any(search_value.lower() in str(value).lower() for value in row)]
    else:
        filtered_data = driving_data

    page_data = filtered_data[start:start + length]

    response = {
        "draw": draw,
        "recordsTotal": len(driving_data),
        "recordsFiltered": len(filtered_data),
        "data": page_data
    }

    return jsonify(response)

@app.route('/get_updated_data', methods=['GET'])
def get_updated_data():
    specific_date = request.args.get('specific_date', default=None)
    if specific_date:
        data = get_driving_speed_data(data_folder_path, specific_date)
        return jsonify(data)
    else:
        return jsonify({})

@socketio.on('client_connected')
def emit_driving_speed(selected_date, time_offset, time_interval):
    global driving_speed_data

    if selected_date:
        driving_speed_data = get_driving_speed_data(data_folder_path, selected_date, time_offset, time_interval)

    driving_speed_data_serializable = [
        {
            "car_plate_number": row["carPlateNumber"],
            "avg_speed": row["avg_speed"],
            "overspeed_count": row["overspeed_count"],
            "records": [
                {
                    "Time": record["Time"].strftime('%Y-%m-%d %H:%M:%S'),
                    "speed": record["Speed"],
                    "isOverspeed": record["isOverspeed"]
                }
                for record in row["records"]
            ]
        }
        for row in driving_speed_data.collect()
    ]

    warnings = [
        {
            "car_plate_number": row["carPlateNumber"],
            "overspeed_count": row["overspeed_count"]
        }
        for row in driving_speed_data.collect()
        if row["overspeed_count"] > 0
    ]

    print(f"Emitting data: {driving_speed_data_serializable}")
    emit('update_chart', driving_speed_data_serializable)
    emit('update_warnings', warnings)


@app.route('/driving_speed')
def driving_speed():
    return render_template('driving_speed.html')

@app.route('/get_earliest_timestamp', methods=['GET'])
def fetch_earliest_timestamp():
    specific_date = request.args.get('specific_date', default=None)
    if specific_date:
        timestamp = get_earliest_timestamp(combined_dataframe, specific_date)
        return jsonify({"timestamp": timestamp})
    else:
        return jsonify({})
    
@socketio.on('selected_date_and_time')
def handle_selected_date_and_time(selected_date, start_time, time_interval):
    data_folder_path = "output_data"
    if not start_time:
        start_time = '2017-01-01 08:00:05'

    start_time = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
    driving_speed_data = get_driving_speed_data(data_folder_path, start_time, time_interval) 
    driving_speed_data_serializable = []

    for row in driving_speed_data.collect():
        car_plate_number = row["carPlateNumber"]
        records = row["records"]

        for data_point in records:
            data_point_serializable = data_point.asDict()
            data_point_serializable["car_plate_number"] = car_plate_number
            data_point_serializable["Time"] = json.dumps(data_point_serializable["Time"], cls=DatetimeEncoder).replace("T", " ")[1:-1]
            driving_speed_data_serializable.append(data_point_serializable)

    emit('update_chart', driving_speed_data_serializable)

@socketio.on('get_driving_speed_data')
def handle_get_driving_speed_data(time_interval, specific_date=None, start_time=None):
    driving_speed_data = get_driving_speed_data(data_folder_path, specific_date, start_time, time_interval)
    emit('driving_speed_data', driving_speed_data)

if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 8080))
    socketio.run(app, host='0.0.0.0', port=port, debug=True)
