import time
import hashlib
import os
import zlib
import zipfile
from typing import Optional

from simplekv.net.botostore import BotoStore
import boto

MB = 10 ** 6

DEFAULTS = dict(
  max_size_per_file=10 * MB,
  min_size_per_file=1 * MB,
  max_files=100,
  max_total_size=250 * MB,
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

class Timer:

  def __init__(self, name: str):
    self.name = name
  
  def __enter__(self):
    self.start = time.perf_counter()
  
  def __exit__(self, *_):
    self.duration = time.perf_counter() - self.start
    print(f'{self.name}: {self.duration:.1f}')

class ReplayDB:

  def __init__(self, name: str = NAME):
    self.name = name
    self.metadata = db.get_collection(name)
    self.params = get_params(name)

  def current_db_size(self) -> int:
    total_size = 0
    for doc in self.metadata.find():
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

    found = self.metadata.find_one({'key': key})
    if found is not None:
      return f'{name}: duplicate file'

    # TODO: validate that file conforms to .slp spec

    # store file in S3
    compressed_bytes = zlib.compress(content)
    store.put(self.name + '.' + key, compressed_bytes)

    # update DB
    self.metadata.insert_one(dict(
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
      num_uploaded = self.metadata.count_documents({})
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

  def upload_fast(self, uploaded, obj_type, key_method='name'):
    name = uploaded.filename
    with Timer('read'):
      content = uploaded.read()
      uploaded.close()

    max_bytes_left = self.max_db_size() - self.current_db_size()
    if len(content) + self.current_db_size() > self.max_db_size():
      return f'{name}: exceeds {max_bytes_left} bytes'

    if key_method == 'name':
      key = name
    elif key_method == 'content':
      with Timer('sha256'):
        digest = hashlib.sha256()
        digest.update(content)
        key = digest.hexdigest()
    else:
      raise ValueError(f'Invalid key_method {key_method}.')

    found = self.metadata.find_one({'key': key})
    if found is not None:
      return f'{name}: duplicate upload'

    with Timer('store.put'):
      store.put(self.name + '.' + key, content)

    # update DB
    self.metadata.insert_one(dict(
      name=name,
      key=key,
      type=obj_type,
      stored_size=len(content),
    ))
    return f'{name}: upload successful'

def nuke_replays(name: str):
  db.drop_collection(name)
  db.params.delete_many({'name': name})
  keys = list(store.iter_keys(prefix=name + '.'))
  for key in keys:
    store.delete(key)
  print(f'Deleted {len(keys)} objects.')
