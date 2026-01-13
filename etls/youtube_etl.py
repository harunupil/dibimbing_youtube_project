import sys
from datetime import datetime, timezone

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from textblob import TextBlob

from utils.constants import VIDEO_FIELDS

# CONNECTION
def connect_youtube(api_key: str):
    try:
        youtube = build(
            serviceName="youtube",
            version="v3",
            developerKey=api_key
        )
        print("Connected to YouTube API")
        return youtube
    except Exception as e:
        print(e)
        sys.exit(1)

# EXCTRACTION
def extract_videos(youtube, query: str, max_results= int):
    request = youtube.search().list(
        part="snippet",
        q=query,
        maxResults=max_results,
        type="video",
        order="relevance"
    )
    response = request.execute()

    video_ids = list({
        item["id"]["videoId"]
        for item in response.get("items", [])
    })

    return video_ids[:max_results]

def get_video_statistics(youtube, video_ids):
    request = youtube.videos().list(
        part="snippet,statistics",
        id=",".join(video_ids)
    )
    response = request.execute()

    videos = []
    for item in response['items']:
        videos.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "description": item["snippet"].get("description", ""),
            "published_at": item["snippet"]["publishedAt"],
            "channel_title": item["snippet"]["channelTitle"],
            "view_count": int(item["statistics"].get("viewCount", 0)),
            "like_count": int(item["statistics"].get("likeCount", 0)),
            "comment_count": int(item["statistics"].get("commentCount", 0)),
        })

    return videos

def extract_comments(youtube, video_id, max_comments):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(max_comments, 100), #Youtube API limitation Max 100
            textFormat="plainText",
            order="relevance"
        )

        while request and len(comments) < max_comments:
            response = request.execute()

            for item in response.get("items", []):
                text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                comments.append(text)

            request = youtube.commentThreads().list_next(request, response)

    except HttpError as e:
        # Comments disabled or forbidden → return empty list
        if e.resp.status == 403:
            print(f"Comments disabled for video {video_id}")
        else:
            raise e  # real error → fail fast

    return comments

# SENTIMENT (AGGREGATED)
def analyze_sentiments(comments: list[str]) -> float:
    
    if not comments:
        return 0.0

    scores = []

    for text in comments:
        try:
            polarity = TextBlob(text).sentiment.polarity
            # Treat NaN or undefined as neutral
            if polarity is None:
                polarity = 0.0
        except Exception:
            polarity = 0.0

        scores.append(polarity)

    return sum(scores) / len(scores)


def transform_data(video_df: pd.DataFrame):

    video_df["video_id"] = video_df["video_id"].astype(str)
    video_df["title"] = video_df["title"].astype(str)
    video_df["channel_title"] = video_df["channel_title"].astype(str)

    #Convert published_at → DATE
    video_df["published_at"] = (
        pd.to_datetime(video_df["published_at"], utc=True)
        .dt.date
    )

    video_df["view_count"] = video_df["view_count"].astype(int)
    video_df["like_count"] = video_df["like_count"].astype(int)
    video_df["comment_count"] = video_df["comment_count"].astype(int)
    video_df["fetch_date"] = datetime.now(timezone.utc).date()

    return video_df


def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)
