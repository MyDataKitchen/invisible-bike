from flask import Flask, render_template
from model import get_marker_data


PORT = 8080
HOST = '0.0.0.0'

app = Flask(__name__)

@app.route("/")
def hello_world():
    return render_template('main.html')


@app.route("/api/1.0/data", methods=['GET'])
def get_station_info():
    data = []
    rows = get_marker_data()
    for row in rows:
        data.append(list(row))

    # print(data)

    return {'data': data}




if __name__ == '__main__':
    app.run(debug=True, port=PORT, host=HOST)