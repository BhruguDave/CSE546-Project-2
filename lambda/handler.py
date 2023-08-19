import urllib.parse
from lambda_util import download_from_s3, extract_frames_from_video

input_bucket = "cse-546-ec3-input-bucket"
output_bucket = "cse-546-ec3-output-bucket"
encoding_path = "./encoding"
temp_path = "/tmp/"


def face_recognition_handler(event, context):
    print("PROCESSING REQUEST")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        filepath = temp_path + str(key)
        download_from_s3(filepath, bucket, key)
        extract_frames_from_video(filepath, temp_path, encoding_path, key, output_bucket)
    except Exception as e:
        print("ERROR: " + e)


# LOCAL TESTING
# temp_path = "/Users/bhrugudave/Downloads/temporary/"
# key = "test_4.mp4"
# filepath = temp_path + str(key)
# download_from_s3(filepath, input_bucket, key)
# extract_frames_from_video(filepath, temp_path, encoding_path, key, output_bucket)


