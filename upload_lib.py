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

class ReplayDB:

  def __init__(self, name: str = NAME):
    self.name = name
    self.metadata = db.get_collection(name)
    self.params = get_params(name)

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

    # update mongo DB
    self.metadata.insert_one(dict(
      key=key,
      name=name,
      size=len(content),
      compressed_size=len(compressed_bytes),
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
