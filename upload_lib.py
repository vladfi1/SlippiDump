import time
import hashlib
import base64
import os
import zlib
import zipfile
from typing import Optional

import boto3
from simplekv.net.boto3store import Boto3Store
import werkzeug.datastructures

MB = 10 ** 6

DEFAULTS = dict(
  max_size_per_file=10 * MB,
  min_size_per_file=1 * MB,
  max_files=100,
  max_total_size=250 * MB,
)

# controls where stuff is stored
NAME = os.environ.get('NAME', 'test')

def get_store() -> Boto3Store:
  s3_creds = os.environ['S3_CREDS']
  access_key, secret_key = s3_creds.split(':')
  session = boto3.Session(access_key, secret_key)
  s3 = session.resource('s3')
  bucket = s3.Bucket('slippi-data')
  return Boto3Store(bucket)

store = get_store()

from pymongo import MongoClient

client = MongoClient(os.environ['MONGO_URI'])
db = client.slp_replays

def get_params(name: str) -> dict:
  params_coll = db.params
  found = params_coll.find_one({'name': name})
  if found is None:
    # update params collection
    params = dict(name=name, **DEFAULTS)
    params_coll.insert_one(params)
    return params
  # update found with default params
  for k, v in DEFAULTS.items():
    if k not in found:
      found[k] = v
  return found

def create_params(name: str, **kwargs):
  assert db.params.find_one({'name': name}) is None
  params = dict(name=name, **DEFAULTS)
  params.update(kwargs)
  db.params.insert_one(params)

class Timer:

  def __init__(self, name: str):
    self.name = name
  
  def __enter__(self):
    self.start = time.perf_counter()
  
  def __exit__(self, *_):
    self.duration = time.perf_counter() - self.start
    print(f'{self.name}: {self.duration:.1f}')

def iter_bytes(f, chunk_size=2 ** 16):
  while True:
    chunk = f.read(chunk_size)
    if chunk:
      yield chunk
    else:
      break
  f.seek(0)

class ReplayDB:

  def __init__(self, name: str = NAME):
    self.name = name
    self.params = get_params(name)
    self.raw = db.get_collection(name + '-raw')

  def raw_size(self) -> int:
    total_size = 0
    for doc in self.raw.find():
      total_size += doc['stored_size']
    return total_size

  @property
  def max_file_size(self):
    return self.params['max_size_per_file']

  @property
  def min_file_size(self):
    return self.params['min_size_per_file']

  @property
  def max_files(self):
    return self.params['max_files']

  def max_db_size(self):
    return self.params['max_total_size']

  def upload_slp(self, name: str, content: bytes) -> Optional[str]:
    # max_files = params['max_files']
    # if coll.count_documents({}) >= max_files:
    #   return f'DB full, already have {max_files} uploads.'
    if not name.endswith('.slp'):
      return f'{name}: not a .slp'
    
    max_size = self.params['max_size_per_file']
    if len(content) > max_size:
      return f'{name}: exceeds {max_size} bytes.'
    min_size = self.params['min_size_per_file']
    if len(content) < min_size:
      return f'{name}: must have {min_size} bytes.'

    digest = hashlib.sha256()
    digest.update(content)
    key = digest.hexdigest()

    found = self.raw.find_one({'key': key})
    if found is not None:
      return f'{name}: duplicate file'

    # TODO: validate that file conforms to .slp spec

    # store file in S3
    compressed_bytes = zlib.compress(content)
    store.put(self.name + '.' + key, compressed_bytes)

    # update DB
    self.raw.insert_one(dict(
      key=key,
      name=name,
      type='slp',
      compressed=True,
      original_size=len(content),
      stored_size=len(compressed_bytes),
    ))

    return None

  def upload_zip(self, uploaded):
    errors = []
    with zipfile.ZipFile(uploaded) as zip:
      names = zip.namelist()
      names = [n for n in names if n.endswith('.slp')]
      print(names)

      max_files = self.params['max_files']
      num_uploaded = self.raw.count_documents({})
      if num_uploaded + len(names) > max_files:
        return f'Can\'t upload {len(names)} files, would exceed limit of {max_files}.'

      for name in names:
        with zip.open(name) as f:
          error = self.upload_slp(name, f.read())
          if error:
            errors.append(error)
    
    uploaded.close()
    if errors:
      return '\n'.join(errors)
    return f'Successfully uploaded {len(names)} files.'

  def upload_raw(
    self,
    uploaded: werkzeug.datastructures.FileStorage,
    obj_type: str,
    description: str,
  ):
    name = uploaded.filename
    f = uploaded.stream
    size = f.seek(0, 2)
    f.seek(0)

    max_bytes_left = self.max_db_size() - self.raw_size()
    if size > max_bytes_left:
      return f'{name}: exceeds {max_bytes_left} bytes'

    with Timer('md5'):
      digest = hashlib.md5()
      for chunk in iter_bytes(f):
        digest.update(chunk)
      key = digest.hexdigest()

    found = self.raw.find_one({'key': key})
    if found is not None:
      return f'{name}: object with md5={key} already uploaded'

    with Timer('store.put'):
      store.bucket.upload_fileobj(
          Fileobj=f,
          Key=self.name + '/raw/' + key,
          # ContentLength=size,
          # ContentMD5=str(base64.encodebytes(digest.digest())),
      )
      # store.put_file(self.name + '/raw/' + key, f)

    # update DB
    self.raw.insert_one(dict(
        filename=name,
        key=key,
        hash_method="md5",
        type=obj_type,
        description=description,
        stored_size=size,
        processed=False,
    ))
    return f'{name}: upload successful'

  def delete(self, key: str):
    s3_key = self.name + '/raw/' + key
    store.bucket.delete_objects(Delete=dict(Objects=[dict(Key=s3_key)]))
    # store.delete()
    self.raw.delete_one({'key': key})

def nuke_replays(name: str, stage='raw'):
  db.drop_collection(name)
  db.params.delete_many({'name': name + '-' + stage})
  keys = list(store.iter_keys(prefix=f'{name}/{stage}/'))
  store.bucket.delete_keys(keys)
  print(f'Deleted {len(keys)} objects.')
