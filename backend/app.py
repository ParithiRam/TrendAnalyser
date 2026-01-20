from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
import os

app = Flask(__name__)
CORS(app)

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(MONGO_URI)
db = client['trending_db']
collection = db['topics']

@app.route('/api/trends', methods=['GET'])
def get_trends():
    try:
        # Get top 10 trending topics sorted by count
        trends = list(collection.find({}, {'_id': 0}).sort('count', -1).limit(10))
        return jsonify(trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
