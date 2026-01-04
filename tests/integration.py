import requests
import pytest
import psycopg2

def test_youtube_api_response(get_airflow_varibles):
    api_key = get_airflow_varibles("API_KEY")
    channel_handle = get_airflow_varibles("CHANNEL_HANDLE")

    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try: 
        response = requests.get(url)
        assert response.status_code == 200

    except requests.exceptions.RequestException as e:
        pytest.fail(f"Request to YouTube API failed: {e}")


def test_real_postgres_connection(real_postgres_connection):
    cursor = None

    try: 
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")


    finally:
        if cursor is not None:
            cursor.close()