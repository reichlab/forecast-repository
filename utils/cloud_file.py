import logging

import boto3

from forecast_repo.settings.base import S3_BUCKET_PREFIX


logger = logging.getLogger(__name__)


#
# This file contains code to handle managing files on a cloud-based service. This is an attempt to abstract away some of
# the service-specific details in case we want to change providers in the future. That said, this file implements the
# functionality in S3.
#
# The types of files currently include temporary forecast csv data file uploads and cached csv score files.
#
# Naming conventions: To simplify the code we use a simple naming convention with a single 'folder' namespace
# (comparable to S3 buckets) and a filename based on the PK of the class of object involved. Naming assumes there's a
# class corresponding to each type of file to be managed. Currently this includes UploadFileJob and ScoreCsvFileCache.
#
# Folder names: To get the folder name we use the corresponding class name in lower case, e.g.,
# UploadFileJob -> 'uploadfilejob'. Note that for S3, this is then used as a postfix to dotted naming convention we've
# adopted, i.e., 'reichlab.zoltarapp.<folder_name>'. These buckets are created manually.
#
# File names: Our filename convention is to use the relevant object's PK to name files, i.e., str(the_obj.pk). Note that
# there is no file extension. Thus, to eliminate name conflicts each class needs its own 'folder' as described above.
#
#
# The currently supported classes:
#
# UploadFileJob:
# - folder name: 'uploadfilejob' (S3 bucket: 'reichlab.zoltarapp.uploadfilejob')
# - filename: UploadFileJob.pk as a string
#
# ScoreCsvFileCache:
# - folder name: 'scorecsvfilecache' (S3 bucket: 'reichlab.zoltarapp.scorecsvfilecache')
# - filename: ScoreCsvFileCache.pk as a string
#


#
# todo:
#   export S3_BUCKET_PREFIX=reichlab.zoltarapp
# - 'rename' S3 buckets:
#   = reichlab.zoltarapp.uploads       -> reichlab.zoltarapp.uploadfilejob
#   = reichlab.zoltarapp.cached-scores -> reichlab.zoltarapp.scorecsvfilecache
#


def folder_name_for_object(the_object):
    """
    Implements the above naming conventions.

    :param the_object: a Model
    """
    return the_object.__class__.__name__


def file_name_for_object(the_object):
    """
    Implements the above naming conventions.

    :param the_object: a Model
    """
    return str(the_object.pk)


def _s3_bucket_name_for_object(the_object):
    return S3_BUCKET_PREFIX + '.' + folder_name_for_object(the_object)


def upload_file(the_object, data_file):
    """
    Uploads data_file to the S3 bucket corresponding to the_object.

    :param data_file: a file-like object
    :param the_object: a Model
    :raises: S3 exceptions
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(_s3_bucket_name_for_object(the_object))
    # todo use chunks? for chunk in data_file.chunks(): print(chunk):

    # bucket.put_object(Key=upload_file_job.s3_key(), Body=data_file, ContentType='text/csv')  # todo xx nope
    bucket.put_object(Key=file_name_for_object(the_object), Body=data_file)


def delete_file(the_object):
    """
    Deletes the S3 object corresponding to the_object. note that we do not log delete failures in the instance. This
    is b/c failing to delete a temporary file is not a failure to process an uploaded file. Though it's not clear when
    delete would fail but everything preceding it would succeed...

    Apps can infer this condition by looking for non-deleted S3 objects whose status != SUCCESS .

    Does nothing if the file does not exist.

    :param the_object: a Model
    """
    try:
        logger.debug("delete_file(): started: {}".format(the_object))
        s3 = boto3.resource('s3')
        s3.Object(_s3_bucket_name_for_object(the_object), file_name_for_object(the_object)).delete()
        logger.debug("delete_file(): done: {}".format(the_object))
    except Exception as exc:
        logger.debug("delete_file(): failed: {}, {}".format(exc, the_object))


def download_file(the_object, data_file):
    """
    Downloads a data_file from the S3 bucket corresponding to the_object into data_file.

    :param the_object: a Model
    :param data_file: a file-like object
    :raises: S3 exceptions
    """
    s3 = boto3.client('s3')  # using client here instead of higher-level resource b/c want to save to a fp
    s3.download_fileobj(_s3_bucket_name_for_object(the_object), file_name_for_object(the_object), data_file)
