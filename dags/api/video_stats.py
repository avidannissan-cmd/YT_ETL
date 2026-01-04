import requests
import json
from datetime import date

from airflow.decorators import task
from airflow.models import Variable

maxResults = 50


@task
def get_playlist_id():

    API_KEY = Variable.get("API_KEY")
    CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")

    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        
        response.raise_for_status()

        data = response.json()

        # print(json.dumps(data,indent=4))

        channel_items = data['items'][0]

        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']

        print(channel_playlist_id)

        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        raise e

@task
def get_video_ids(playlistID): 

    API_KEY = Variable.get("API_KEY")

    video_ids = []

    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlistID}&maxResults={maxResults}&key={API_KEY}"       

    try:

        while True:

            url = base_url 
            
            if pageToken:
                url += f"&pageToken={pageToken}" 

            response = requests.get(url)
            
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')

            if not pageToken:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e   

def batch_list(video_id_list, batch_size):
    for i in range(0, len(video_id_list), batch_size):
        yield video_id_list[i:i+batch_size]


@task
def extract_video_data(video_id_list, batch_size=50):
    API_KEY = Variable.get("API_KEY")
    extracted_data = []

    try:
        for batch in batch_list(video_id_list, batch_size):
            video_ids_str = ','.join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)
            
            response.raise_for_status()

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    'video_id': video_id,
                    'video_title': snippet['title'],  # Fixed: changed from 'title' to 'video_title'
                    'upload_date': snippet['publishedAt'],  # Fixed: changed from 'publishedAt' to 'upload_date'
                    'duration': contentDetails['duration'],
                    'video_views': int(statistics.get('viewCount', 0)) if statistics.get('viewCount') else None,  # Fixed: changed from 'viewCount' to 'video_views' and convert to int
                    'likes_count': int(statistics.get('likeCount', 0)) if statistics.get('likeCount') else None,  # Fixed: changed from 'likeCount' to 'likes_count' and convert to int
                    'comments_count': int(statistics.get('commentCount', 0)) if statistics.get('commentCount') else None  # Fixed: changed from 'commentCount' to 'comments_count' and convert to int
                }

                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"       

    with open(file_path, 'w', encoding='utf-8') as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    playlistID = get_playlist_id()
    video_ids = get_video_ids(playlistID)
    video_data = extract_video_data(video_ids, batch_size=50)
    save_to_json(video_data)