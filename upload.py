import os

from flask import app, Flask, request

import upload_lib

replay_db = upload_lib.ReplayDB(upload_lib.NAME)
app = Flask(upload_lib.NAME)

home_html = """
<html>
  Upload a single slippi replay (.slp) or a zipped collection (.zip) of replays.
  <br/>
  Currently have {num_mb} MB uploaded to database "{db}".
  <br/>
  <body>
    <form action = "http://localhost:5000/upload" method = "POST" 
      enctype = "multipart/form-data">
      <input type = "file" name = "file" />
      <input type = "submit"/>
    </form>
  </body>
</html>
"""

@app.route('/')
def homepage():
  return home_html.format(
    num_mb=replay_db.current_db_size() // upload_lib.MB,
    db=upload_lib.NAME,
  )

@app.route('/upload', methods = ['POST'])
def upload_file():
  f = request.files['file']
  if f.filename.endswith('.slp'):
    max_files = replay_db.params['max_files']
    num_uploaded = replay_db.metadata.count_documents({})
    if replay_db.metadata.count_documents({}) >= max_files:
      return f'DB full, already have {num_uploaded} uploads.'

    error = replay_db.upload_slp(f.filename, f.read())
    f.close()
    return error or f'{f.filename}: upload successful'
  elif f.filename.endswith('.zip'):
    # return replay_db.upload_zip(f)
    return replay_db.upload_fast(f, obj_type='zip', key_method='content')
  else:
    return f'{f.filename}: must be a .slp or .zip'

if __name__ == '__main__':
  app.run(host='0.0.0.0', debug = True)
