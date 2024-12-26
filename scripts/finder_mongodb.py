import json
import sys
from connections import get_mongo_client

number = int(sys.argv[1])
katm = str(sys.argv[2])

client = get_mongo_client()
db = client['task']
task_collection = db['task']

query = {
    f'data.{katm}': {'$exists': True},
    'number': {'$eq': number}
}
projection = {
    'number': 1,
    f'data.{katm}': 1
}
docs = task_collection.find(query, projection)

json_tree = {"output": list(docs)}
with open("output.json", "w", encoding="utf-8") as json_file:
    json.dump(json_tree, json_file, indent=4, default=str, ensure_ascii=False)
