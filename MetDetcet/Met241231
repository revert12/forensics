#!/usr/bin/python3
import json
import csv
import http.client as httplib
import ssl
import time
from datetime import datetime
import os
import mimetypes

# SSL 설정
ssl._create_default_https_context = ssl._create_unverified_context

# 서버 정보
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 8000
USER_ID = "admin"
USER_PW = "admin"
VIDEO_FILE_NAMES = ['./1280x720_15fps.mp4']

# CSV 파일 생성
def create_csv():
    fieldnames = ['id', 'speed', 'CPU', 'GPU', 'timestamp']
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    file_name = f"MetDetect_{timestamp}.csv"
    print(f"CSV 파일 생성: {file_name}")
    with open(file_name, "w", newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        return writer

# UTC 현재 시간 반환
def get_utc_time():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

# 로그인 및 토큰 반환
def login():
    endpoint = "/v1/auth/login"
    payload = {"id": USER_ID, "pw": USER_PW}
    headers = {"Content-Type": "application/json", 'Connection': 'keep-alive'}
    response = send_request("POST", endpoint, headers, payload)
    return response.get('token')

# 헤더 생성
def create_headers(token):
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

# 서버 요청 처리
def send_request(method, endpoint, headers, payload=None, is_multipart=False):
    try:
        conn = httplib.HTTPSConnection(SERVER_HOST, SERVER_PORT)

        if is_multipart:
            conn.request(method, endpoint, payload, headers)
        else:
            conn.request(method, endpoint, json.dumps(payload) if payload else None, headers)

        response = conn.getresponse()
        data = response.read().decode('utf-8')
        if response.status >= 400:
            raise Exception(f"요청 실패: {response.status} {response.reason}")
        
        return json.loads(data) if data else {}

    except Exception as e:
        print(f"요청 중 오류 발생: {e}")
        raise
    finally:
        conn.close()

# 비디오 파일 읽기
def read_video_file(video_file_path):
    with open(video_file_path, 'rb') as video_file:
        video_data = video_file.read()
        video_content_type = mimetypes.guess_type(video_file_path)[0] or 'application/octet-stream'
    return video_data, video_content_type

# 멀티파트 바디 생성
def create_multipart_body(fields, video_data, video_content_type, boundary):
    body = []
    for key, value in fields.items():
        body.append(f"--{boundary}")
        body.append(f'Content-Disposition: form-data; name="{key}"')
        body.append('')
        body.append(value)

    body.append(f"--{boundary}")
    body.append(f'Content-Disposition: form-data; name="video_files"; filename="{os.path.basename(fields["name"])}"')
    body.append(f"Content-Type: {video_content_type}")
    body.append('')
    body.append(video_data)
    body.append(f"--{boundary}--")
    body.append('')
    
    return b"\r\n".join(
        part.encode('latin1') if isinstance(part, str) else part for part in body
    )

# 비디오 데이터 전송
def send_video_data(headers, video_file_path):
    if not os.path.exists(video_file_path):
        raise FileNotFoundError(f"비디오 파일이 존재하지 않습니다: {video_file_path}")

    endpoint = "/v1/extractor/add/channel"
    boundary = '----WebKitFormBoundary7MA4YWxkTrZu0gW'
    start_time = get_utc_time()

    fields = {
        'name': os.path.basename(video_file_path),
        'start_time': start_time,
        'vca_license_codes': '6535',
        'forensic_license_codes': '6537',
        'mode': '3'
    }

    video_data, video_content_type = read_video_file(video_file_path)
    body = create_multipart_body(fields, video_data, video_content_type, boundary)
    headers['Content-Type'] = f"multipart/form-data; boundary={boundary}"

    response = send_request("POST", endpoint, headers, body, is_multipart=True)
    
    return response.get('id')

# MetDetect 정보 가져오기
def get_met_detect_info(headers):
    return send_request("GET", "/v1/extractor/info", headers)

# 하드웨어 리소스 정보 가져오기
def get_hw_resources(headers):
    data = send_request("GET", "/v1/settings/general/resources", headers)
    return data['cpu']['total'], data['gpu'][0]['usage']

# 비디오 삭제
def delete_video(video_id):
    try:
        token = login()
        headers = create_headers(token)
        payload = {"extractor_ids": [video_id]}
        print(f"요청 페이로드: {payload}")  # 페이로드 출력
        print(f"요청 헤더: {headers}")      # 헤더 출력
        response = send_request("DELETE", "/v1/extractor/delete", headers, payload)
        print(f"서버 응답: {response}")     # 응답 출력
        print(f"비디오 {video_id} 삭제 성공")
        return True
    except Exception as e:
        print(f"비디오 {video_id} 삭제 실패: {e}")
        return False

# 주기적 요청 및 데이터 처리
def process_video_data(headers, writer, video_files):
    for video_file in video_files:
        for attempt in range(10):  # 10번 반복
            video_id = send_video_data(headers, video_file)
            
            while True:
                try:
                    met_detect_info = get_met_detect_info(headers)
                    if met_detect_info[0].get('extractor_process_end_time') is None:
                        cpu_total, gpu_total = get_hw_resources(headers)
                        total_speed = met_detect_info[0].get('speed')
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        print(f"--- {attempt + 1} ---")
                        print(f"Timestamp: {timestamp}")
                        print(f"Total Speed: {total_speed}")
                        print(f"CPU Usage: {cpu_total}")
                        print(f"GPU Usage: {gpu_total}")
                        
                        writer.writerow({'id': video_id, 'speed': total_speed, 'CPU': cpu_total, 'GPU': gpu_total, 'timestamp': timestamp})
                        time.sleep(5)  # 일정 시간 기다리고 다시 확인
                    else:
                        time.sleep(10)
                        print("처리가 완료되었습니다. 비디오 삭제를 진행합니다.")
                        writer.writerow({'id': video_id, 'timestamp': 'End'})
                        delete_video(video_id)
                        break
                except Exception as e:
                    print(f"오류 발생: {e}")
                    break

# 메인 함수
def main():
    try:
        writer = create_csv()
        token = login()
        headers = create_headers(token)

        process_video_data(headers, writer, VIDEO_FILE_NAMES)
    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    main()
