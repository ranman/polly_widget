import hashlib
import os
import tempfile
import time

import boto3
import goose
import unidecode

MAX_CHARS = os.getenv("MAX_CHARS", 1500)
SAMPLE_RATE = os.getenv("SAMPLE_RATE", "16000")
BUCKET_NAME = os.getenv("BUCKET_NAME", "polly-widget")
FILE_FORMAT = os.getenv("FILE_FORMAT", "mp3")
CACHE_TIME = os.getenv("CACHE_TIME", 3600)
s3 = boto3.client("s3")
s3r = boto3.resource("s3")
polly = boto3.client("polly")
ddb = boto3.resource("dynamodb").Table(os.getenv("TABLE_NAME", "websites"))
g = goose.Goose()

try:
    os.makedirs('/tmp/bin')
except:
    pass

s3r.Object(BUCKET_NAME, "ffmpeg").download_file('/tmp/bin/ffmpeg')
os.chmod('/tmp/bin/ffmpeg', 755)

from pydub import AudioSegment

AudioSegment.converter = "/tmp/bin/ffmpeg"


def build_composite_array(content, max_chars):
    """Split on words into < MAX_CHARS character points"""
    composite = ['']
    index = 0
    curr_len = 0
    for word in content.split(' '):
        if curr_len+len(word) > max_chars:
            index += 1
            curr_len = len(word) + 1
            composite.append(word + ' ')
        else:
            curr_len += len(word) + 1
            composite[index] += word + ' '
    return [unidecode.unidecode(comp) for comp in composite]


def build_wave_file(content, voice='Justin'):
    """Build a wave file from a group of text objects"""
    sound = AudioSegment.empty()
    for text in content:
        resp = polly.synthesize_speech(
            OutputFormat="mp3",
            SampleRate=SAMPLE_RATE,
            Text=text,
            TextType='text',
            VoiceId=voice
        )
        pth = "/tmp/{}".format(resp['ResponseMetadata']['RequestId'])
        with open(pth, "wb") as f:
            f.write(resp['AudioStream'].read())
        sound += AudioSegment.from_mp3(pth)
    fd, tmp_path = tempfile.mkstemp(dir="/tmp")
    sound.export(tmp_path, format=FILE_FORMAT)
    with open(tmp_path, 'rb') as fp:
        sound_data = fp.read()
    return sound_data


def generate_hash(content):
    hasher = hashlib.md5()
    hasher.update(content)
    return hasher.hexdigest()


def lambda_handler(event, context):
    url = event.get('url')
    voice = event.get('voice', 'Joanna')
    if not url:
        raise ValueError("Bad Request: missing url")
    resp = ddb.get_item(Key={'url': url})
    if resp.get('Item') and (resp['Item']['ts'] + CACHE_TIME < time.time()):
        return resp['Item']
    article = g.extract(url=url)
    content = build_composite_array(article.cleaned_text, MAX_CHARS)
    polly_data = build_wave_file(content, voice)
    md5 = generate_hash(polly_data)
    s3.put_object(Bucket=BUCKET_NAME, ACL='public-read', Body=polly_data, Key=md5)
    item = {
        'url': url,
        'md5': md5,
        'ts': int(time.time()),
        's3': "{}/{}/{}".format(s3.meta.endpoint_url, BUCKET_NAME, md5)
    }
    ddb.put_item(Item=item)
    return item
