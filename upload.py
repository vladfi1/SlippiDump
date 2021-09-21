import hashlib
import os
import zlib

from flask import Flask, request

from simplekv.net.botostore import BotoStore
import boto

MB = 10 ** 6

DEFAULTS = dict(
  max_size_per_file=10 * MB,
  max_files=100,
)

# controls where stuff is stored
NAME = os.environ.get('NAME', 'test')

def get_store() -> BotoStore:
  s3_creds = os.environ['S3_CREDS']
  access_key, secret_key = s3_creds.split(':')
  con = boto.connect_s3(access_key, secret_key)
  bucket = con.get_bucket('slp-replays')
  return BotoStore(bucket)

store = get_store()

from pymongo import MongoClient, collation

client = MongoClient(os.environ['MONGO_URI'])
db = client.slp_replays
coll = db.get_collection(NAME)

def get_params() -> dict:
  params_coll = db.params
  found = params_coll.find_one({'name': NAME})
  if found is None:
    params = dict(name=NAME, **DEFAULTS)
    params_coll.insert_one(params)
    return params
  return found

params = get_params()

app = Flask(NAME)

home_html = """
<html>
   <body>
      <form action = "http://localhost:5000/upload_single" method = "POST" 
         enctype = "multipart/form-data">
         <input type = "file" name = "file" />
         <input type = "submit"/>
      </form>   
   </body>
</html>
"""

@app.route('/')
def homepage():
  return home_html

@app.route('/upload_single', methods = ['POST'])
def upload_single():
  max_files = params['max_files']
  if coll.count_documents({}) >= max_files:
    return f'DB full, already have {max_files} uploads.'

  f = request.files['file']
  file_bytes = f.read()
  f.close()

  max_size = params['max_size_per_file']
  if len(file_bytes) > max_size:
    return f'Upload must be at most {max_size} bytes.'

  digest = hashlib.sha256()
  digest.update(file_bytes)
  key = digest.hexdigest()

  found = coll.find_one({'key': key})
  if found is not None:
    return 'Duplicate file.'

  # TODO: validate that file conforms to .slp spec

  compressed_bytes = zlib.compress(file_bytes)
  store.put('test.' + key, compressed_bytes)

  coll.insert_one(dict(
    key=key,
    name=f.filename,
    size=len(file_bytes),
    compressed_size=len(compressed_bytes),
  ))

  return 'file uploaded successfully'

if __name__ == '__main__':
  app.run(debug = True)
