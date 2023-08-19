import csv
import boto3
import pickle
import os
import shutil
import face_recognition

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('cse546-ec3-student-data')


def extract_frames_from_video(file_path, temp_path, encoding_file_path, key, output_bucket):
    print("EXTRACTING FRAMES FROM VIDEO")
    os.system("ffmpeg -i " + str(file_path) + " -r 1 " + str(temp_path) + "image-%3d.jpeg")
    lst = os.listdir(temp_path)
    encodings = open_encoding(encoding_file_path)
    loop_flag = False
    for i in range(1, len(lst)):
        image = face_recognition.load_image_file(temp_path + "image-" + "{:03d}".format(i) + ".jpeg")
        image_encoding = face_recognition.face_encodings(image)[0]
        for name, encoding in zip(encodings['name'], encodings['encoding']):
            result = face_recognition.compare_faces([encoding], image_encoding)
            if result[0]:
                response = search_dynamodb_for_entry(name)
                file_path, file_name = create_csv_from_json(response, temp_path, key.split(".")[0])
                upload_to_s3(file_path, output_bucket, file_name)
                clear_temporary_directory(temp_path)
                loop_flag = True
                break

        if loop_flag:
            break

    return


def create_csv_from_json(json_data, temp_path, file_name):
    del json_data['id']
    data_file = open(temp_path + file_name + ".csv", 'w', newline='')
    csv_writer = csv.writer(data_file)
    header = json_data.keys()
    csv_writer.writerow(header)
    csv_writer.writerow(json_data.values())
    data_file.close()

    return temp_path + file_name + ".csv", file_name + ".csv"


def upload_to_s3(file_path, bucket_name, key):
    try:
        s3.upload_file(file_path, bucket_name, key)
        print("UPLOADED RESULT CSV FILE TO S3")
    except Exception as e:
        print("ERROR WHILE UPLOADING TO S3: " + str(e))


def download_from_s3(file_path, bucket_name, key):
    try:
        s3.download_file(Bucket=bucket_name, Key=key, Filename=file_path)
        print("DOWNLOADED FILE FROM S3")
    except Exception as e:
        print("ERROR WHILE DOWNLOADING FROM S3: " + str(e))


def search_dynamodb_for_entry(name):
    try:
        response = table.get_item(
            Key={'name': name}
        )
        return response['Item']
    except Exception as e:
        print("ERROR FETCHING DYNAMODB: " + str(e))


def open_encoding(filename):
    file = open(filename, "rb")
    data = pickle.load(file)
    file.close()
    return data


def clear_temporary_directory(folder):
    print("CLEARING TEMPORARY DIRECTORY")
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("ERROR: " + str(e))
