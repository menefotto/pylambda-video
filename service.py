# -*- codinpg: utf-8 -*-
from __future__ import print_function
from subprocess import check_call
from urllib import unquote_plus
import os
import shutil
import boto3
import time

TMP = "/tmp"
CMD = TMP + "/ffmpeg"
VIDEO_DIR = TMP + "/videos"
BIT_RATE = 1000  # 1000Kpbs

s3 = boto3.client("s3")
print("Loading function")


def handler(event, context):
    """
        This aws lambda function takes a video from a bucket in reaction to
        a bucket put in the videos/ prefix and convert the video to and mp4
        with 1000Kpbs bit rate. and put it back on the bucket.
    """
    message = {"error": None}

    bucket, key, fin, fout = get_info_from(event)
    ret = exec_wrap(s3.download_file)(message, bucket, key, fin)
    if ret["error"] != None:
        return ret

    print("Starting file conversion.")
    t1 = time.time()
    try:
        if not os.path.exists(CMD):
            shutil.copy("ffmpeg", CMD)
            os.chmod(CMD, 0o775)

        check_call([CMD, "-loglevel", "error", "-i", fin, "-b:v", "1M", fout])
        os.remove(fin)
    except Exception as e:
        message["error"] = e
        return message
    t2 = time.time()
    print("Performed compression to 1Kbps in: {0:.2f}s.".format(t2 - t1))

    vname = "{}{}{}".format("videos/", BIT_RATE, key.split("/")[1])
    ret = exec_wrap(s3.upload_file)(message, bucket, vname, None, fout)
    if ret["error"] != None:
        return ret

    return message


def get_info_from(event):
    bucket = event['Records'][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])

    fin = "{}/{}".format(TMP, key.split("/")[1])
    fout = "{}/{}{}".format(VIDEO_DIR, BIT_RATE, key.split("/")[1])

    return bucket, key, fin, fout


def exec_wrap(func):
    func_name = func.func_name.replace("_", " ")

    def func_wrapper(msg, bucket, key, fin=None, fout=None):
        print("Starting to {}.".format(func_name))
        t1 = time.time()
        try:
            if not os.path.exists(VIDEO_DIR):
                os.mkdir(VIDEO_DIR)
            if func.im_func.func_name == "upload_file":
                func(fout, bucket, key)
                os.remove(fout)
            elif func.im_func.func_name == "download_file":
                func(bucket, key, fin)
        except Exception as e:
            msg["error"] = e
        t2 = time.time()
        print("Performed {} in: {:.2f}s.".format(func_name, t2 - t1))

        return msg
    return func_wrapper
