import requests
import time
import json
import isodate
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from concurrent.futures import ThreadPoolExecutor

API_KEY = 'AIzaSyDBfT6kJ9Hz_NJqYoyUrFi0kux4Sfk_Lg8'
BASE_URL = 'https://www.googleapis.com/youtube/v3/'

NO_VIDEO = 2000  # Total videos to fetch

def fetch_categories(region_code='IN'):
    """Fetch video categories in a specific region."""
    url = f"{BASE_URL}videoCategories"
    params = {
        'part': 'snippet',
        'regionCode': region_code,
        'key': API_KEY,
    }
    response = requests.get(url, params=params).json()
    categories = {item['id']: item['snippet']['title'] for item in response.get('items', []) if item['snippet']['assignable']}
    return categories

def fetch_video_details(video_ids):
    """Fetch detailed metadata, including all necessary fields for video recommendation."""
    videos_url = f"{BASE_URL}videos"
    params = {
        'part': 'snippet,contentDetails,statistics',
        'id': ','.join(video_ids),
        'key': API_KEY,
    }
    response = requests.get(videos_url, params=params).json()
    video_details = []

    for item in response.get('items', []):
        try:
            video_id = item['id']
            snippet = item['snippet']
            content_details = item.get('contentDetails', {})
            statistics = item.get('statistics', {})

            # # Parse duration
            # duration_iso = content_details.get('duration')
            # if not duration_iso:
            #     continue
            # duration_minutes = isodate.parse_duration(duration_iso).total_seconds() / 60

            # # Fetch statistics
            # views = int(statistics.get('viewCount', 0))
            # likes = int(statistics.get('likeCount', 0)) if 'likeCount' in statistics else None
            # dislikes = int(statistics.get('dislikeCount', 0)) if 'dislikeCount' in statistics else None
            # comment_count = int(statistics.get('commentCount', 0)) if 'commentCount' in statistics else None
            # like_to_view_ratio = round(likes / views, 4) if views > 0 and likes is not None else None

            # Fetch snippet details
            title = snippet['title']
            description = snippet.get('description', "")
            category_id = snippet.get('categoryId', "Unknown")
            tags = snippet.get('tags', [])
            # published_at = snippet.get('publishedAt', "")
            # channel_title = snippet.get('channelTitle', "")
            # channel_id = snippet.get('channelId', "")
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            # Append all details
            video_details.append({
                'video_id': video_id,
                'title': title,
                'description': description,
                'category_id': category_id,
                # 'duration_minutes': duration_minutes,
                # 'views': views,
                # 'likes': likes,
                # 'dislikes': dislikes,
                # 'like_to_view_ratio': like_to_view_ratio,
                # 'comment_count': comment_count,
                'tags': tags,
                # 'published_at': published_at,
                # 'channel_title': channel_title,
                # 'channel_id': channel_id,
                'video_url': video_url,
            })
        except Exception as e:
            print(f"Skipping video due to error: {e}")
            continue  # Skip this video if any issue occurs

    return video_details


def fetch_transcript(video_id):
    """Fetch the transcript of a video."""
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        return " ".join([entry['text'] for entry in transcript])
    except (TranscriptsDisabled, NoTranscriptFound):
        return None

def fetch_transcripts_parallel(videos):
    """Fetch transcripts in parallel for a list of videos."""
    with ThreadPoolExecutor() as executor:
        # Submit tasks for each video to fetch the transcript in parallel
        futures = {video['video_id']: executor.submit(fetch_transcript, video['video_id']) for video in videos}

        # Wait for each task to complete and update the videos with their transcript
        for video in videos:
            video['transcript'] = futures[video['video_id']].result()
    return videos

def fetch_videos_by_category(category_id, max_results=NO_VIDEO/10):
    """Fetch videos for a specific category ID, limiting to max_results."""
    search_url = f"{BASE_URL}search"
    videos = []
    next_page_token = None
    batch_size = 50  # YouTube API allows max 50 per request

    while len(videos) < max_results:
        params = {
            'part': 'snippet',
            'videoCategoryId': category_id,
            'type': 'video',
            'regionCode': 'IN',
            'maxResults': batch_size,
            'key': API_KEY,
            'pageToken': next_page_token,
        }
        response = requests.get(search_url, params=params).json()
        search_results = response.get('items', [])

        # Extract video IDs and fetch details
        video_ids = [item['id']['videoId'] for item in search_results if 'videoId' in item['id']]
        video_details = fetch_video_details(video_ids)

        for video in video_details:
            # Filter by duration (5 to 15 minutes)
            if 3 <= video['duration_minutes'] <= 15:
                videos.append(video)

        next_page_token = response.get('nextPageToken')

        if not next_page_token or len(videos) >= max_results:
            break

    return videos[:max_results]


def fetch_videos(categories_to_fetch, max_results_per_category=NO_VIDEO):
    """Fetch videos from specific categories."""
    categories = {'1': 'Film & Animation', '2': 'Autos & Vehicles', '10': 'Music', '15': 'Pets & Animals', '17': 'Sports', '19': 'Travel & Events', '20': 'Gaming', '22': 'People & Blogs', '23': 'Comedy', '24': 'Entertainment', '25': 'News & Politics', '26': 'Howto & Style', '27': 'Education', '28': 'Science & Technology', '29': 'Nonprofits & Activism'}
    videos = []

    # Fetch videos for the given categories
    for category_id in categories_to_fetch:
        print(f"Fetching videos for category: {categories[category_id]} (ID: {category_id})")
        category_videos = fetch_videos_by_category(category_id, max_results=max_results_per_category)
        for video in category_videos:
            video['category'] = categories[category_id]  # Add category name
        videos.extend(category_videos)

        # Stop if 10,000 videos are fetched
        if len(videos) >= NO_VIDEO:
            break

    return videos

# Categories to fetch videos from (example: categories 1, 10, 17, etc.)
categories_to_fetch = ['1','2','10',  '20', '25','26', '24', '27']  # You can modify this list as needed
videos = fetch_videos(categories_to_fetch)

# Fetch transcripts for the videos in parallel
# videos_with_transcripts = fetch_transcripts_parallel(videos)

def save_videos_to_file(videos_data, filename='videos_metadata.json'):
    try:
        # Read existing data from the file if it exists
        with open(filename, 'r') as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # If the file does not exist or is empty, start with an empty list
        existing_data = []

    # Append new data to the existing data
    existing_data.extend(videos_data)

    # Write the updated list back to the file
    with open(filename, 'w') as f:
        json.dump(existing_data, f, indent=4)

# Save data in chunks
chunk_size = 50
for i in range(0, len(videos_with_transcripts), chunk_size):
    save_videos_to_file(videos_with_transcripts[i:i + chunk_size])
    print("50 rows added")

print(f"Fetched {len(videos_with_transcripts)} videos with transcripts.")


print(f"Fetched {len(videos_with_transcripts)} videos with transcripts.")
