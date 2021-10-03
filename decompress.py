import hashlib
import tempfile
import zipfile
import zlib

import upload_lib

def _md5(b: bytes) -> str:
  return hashlib.md5(b).hexdigest()

def process_upload(name: str, raw_key: str):
  with upload_lib.Timer("raw_db"):
    raw_db = upload_lib.db.get_collection(name + '-raw')

    raw_info = raw_db.find_one({'key': raw_key})
    obj_type = raw_info['type']
    if obj_type != 'zip':
      return 'Unsupported obj_type={obj_type}.'

  slp_db = upload_lib.db.get_collection(name + '-slp')
  slp_keys = set(doc["key"] for doc in slp_db.find({}, ["key"]))

  tmp = tempfile.TemporaryFile()
  raw_s3_path = f'{name}/raw/{raw_key}'
  with upload_lib.Timer("download_fileobj"):
    upload_lib.store.bucket.download_fileobj(raw_s3_path, tmp)

  tmp.seek(0)
  zf = zipfile.ZipFile(tmp)

  for info in zf.infolist():
    if not info.filename.endswith('.slp'):
      continue

    with upload_lib.Timer("zf.read"):
      slp_bytes = zf.read(info)
    slp_key = _md5(slp_bytes)

    if slp_key in slp_keys:
      print('Duplicate slp with key', slp_key)
      continue
    slp_keys.add(slp_key)

    slp_s3_path = f'{name}/slp/{slp_key}'
    with upload_lib.Timer("zlib.compress"):
      compressed_slp_bytes = zlib.compress(slp_bytes)
    with upload_lib.Timer("put_object"):
      upload_lib.store.bucket.put_object(
          Key=slp_s3_path,
          Body=compressed_slp_bytes)

    slp_db.insert_one(dict(
        filename=info.filename,
        compression='zlib',
        key=slp_key,
        raw_key=raw_key,
        original_size=len(slp_bytes),
        stored_size=len(compressed_slp_bytes),
    ))
