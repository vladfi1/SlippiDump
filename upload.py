import hashlib
import os
import zlib

from flask import Flask, render_template, request
from werkzeug.utils import secure_filename

from simplekv.net.botostore import BotoStore
import boto

def get_store() -> BotoStore:
  s3_creds = os.environ['S3_CREDS']
  access_key, secret_key = s3_creds.split(':')
  con = boto.connect_s3(access_key, secret_key)
  bucket = con.get_bucket('slp-replays')
  return BotoStore(bucket)

store = get_store()

from pymongo import MongoClient

def get_db():
  client = MongoClient(os.environ['MONGO_URI'])
  return client.slp_replays.test

db = get_db()

app = Flask(__name__)

@app.route('/')
def homepage():
  return render_template('upload.html')

@app.route('/upload_single', methods = ['POST'])
def upload_single():
  f = request.files['file']
  file_bytes = f.read()
  f.close()

  digest = hashlib.sha256()
  digest.update(file_bytes)
  key = digest.hexdigest()

  found = db.find_one({'key': key})
  if found is not None:
    return 'Duplicate file.'

  compressed_bytes = zlib.compress(file_bytes)
  store.put('test.' + key, compressed_bytes)

  db.insert_one(dict(
    key=key,
    name=f.filename,
    size=len(file_bytes),
    compressed_size=len(compressed_bytes),
  ))

  return 'file uploaded successfully'

if __name__ == '__main__':
  app.run(debug = True)
