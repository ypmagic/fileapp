import functools
import boto3
import botocore
import os
from botocore.exceptions import ClientError

from flask import (
    Flask, Blueprint, flash, g, redirect, render_template, request, session, url_for, send_file
)
from werkzeug.utils import secure_filename

from fileup.db import get_db
from fileup.auth import login_required

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
UPLOAD_FOLDER = 'temp'
BASE_PATH = os.path.dirname(__file__)
bp = Blueprint('files', __name__, url_prefix='/files')
S3_BUCKET = 'fileuploadypmagic'
app = Flask(__name__)

@bp.route('/fileupload', methods=['POST', 'GET'])
@login_required
def file_upload():
    """
    Handles user file upload, sending the file to S3, then deleting it locally.
    """
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file.')
            return redirect(request.url)
        file = request.files['file']

        if file.filename == '':
            flash('File name is empty.')
            return redirect(request.url)

        if file and allowed_file(file.filename):

            secure_file_name = secure_filename(file.filename)
            secure_file_path = os.path.join(BASE_PATH, (os.path.join(UPLOAD_FOLDER, secure_file_name)))
            file.save(secure_file_path)

            # upload the file to S3 cloud
            if not upload_file(file.filename, secure_file_name, secure_file_path):
                flash('Error: Could not upload file')

            # delete the file from local server
            os.remove(secure_file_path)

            # flash success msg and redirect to the file upload
            flash('Upload successful')
            return redirect(request.url)
    
    return render_template('files/fileupload.html')
        
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def upload_file(file_name, secure_file_name, secure_file_path):
    """Upload a file to fileuploadypmagic bucket in AWS S3.

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    db = get_db()
    
    # Upload the file
    aws_session = boto3.Session(profile_name="Profile1")
    s3_client = aws_session.client('s3')
    try:
        response = s3_client.upload_file(secure_file_path, S3_BUCKET, secure_file_name)

        # in the case that upload succeeds, we want to create a
        # local database entry in the files table.
        db.execute(
            'INSERT INTO files (user, file_name, secure_file_name) VALUES (?, ?, ?)', 
            (session['user_id'], file_name, secure_file_name)
        )
        db.commit()
    except ClientError as e:
        logging.error(e)
        return False
    return True

@bp.route('/fileslist', methods=['GET'])
@login_required
def get_files_list():
    db = get_db()

    def generate_dict(rows):
        for row in rows:
            yield dict(itertools.izip(field_names, row))

    # get all files for this user
    files = db.execute(
        'SELECT * FROM files WHERE user = ?', (session['user_id'],)
    ).fetchall()
    
    # delete all temp files (no background processes / threads allowed in flask)
    for filename in os.listdir(os.path.join(BASE_PATH, UPLOAD_FOLDER)):
        file_path = os.path.join(BASE_PATH, os.path.join(UPLOAD_FOLDER, filename))
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

    return render_template('files/fileslist.html', file_list=files)

@bp.route('/<int:id>')
@login_required
def download_file(id):
    db = get_db()

    # get file details from database
    file = db.execute(
        'SELECT * FROM files WHERE file_id = ?', (id,)
    ).fetchone()

    # file is null or user doesn't match
    if not file or not file['user'] == session['user_id']:
        flash('Insufficient permissions.')
        return redirect(url_for('files.get_files_list'))

    secure_file_name = file['secure_file_name']

    # check if file is already on local server
    if os.path.isfile(os.path.join(BASE_PATH, os.path.join(UPLOAD_FOLDER, secure_file_name))):
        return send_file("temp/" + secure_file_name, as_attachment=True)
    
    # retrieve the file from S3
    aws_session = boto3.Session(profile_name="Profile1")
    s3_client = aws_session.client('s3')
    
    # download file
    try:
        s3_client.download_file(S3_BUCKET, secure_file_name, os.path.join(BASE_PATH, os.path.join(UPLOAD_FOLDER, secure_file_name)))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            flash('File does not exist.')

            # since the file does not exist, delete this data entry from files table
            db.execute(
                'DELETE FROM files WHERE file_id = ?', (id,)
            )
            db.commit()

            return redirect(url_for('files.get_files_list'))
        else:
            raise

    # send the download
    return send_file("temp/" + secure_file_name, as_attachment=True)