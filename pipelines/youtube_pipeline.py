import pandas as pd
from datetime import datetime, timezone
from etls.youtube_etl import (
    connect_youtube,
    extract_videos,
    get_video_statistics,
    extract_comments,
    analyze_sentiments,
    transform_data,
    load_data_to_csv
)
from utils.constants import YOUTUBE_API_KEY, OUTPUT_PATH


def youtube_pipeline(file_name: str, queries: list, max_results= int, max_comments = int):
    youtube = connect_youtube(YOUTUBE_API_KEY)

    all_videos = []
    #Trigger Multiple API Call for all Queries Subject
    for query in queries:
        video_ids = extract_videos(youtube, query, max_results)
        videos = get_video_statistics(youtube, video_ids)

        for video in videos:
            comments = extract_comments(youtube, video["video_id"], max_comments)

            # analyze_sentiments (pos, neg, neu, avg)
            avg = analyze_sentiments(comments)

            #Add new column of AVG Sentiment and Subject for each Query Subject
            video["avg_top_comments_sentiment"] = avg
            video["subject"] = query
            all_videos.append(video)

    video_df = pd.DataFrame(all_videos)
    video_df = transform_data(video_df)

    final_df = video_df[
        [
            "video_id",
            "title",
            "published_at",
            "channel_title",
            "view_count",
            "like_count",
            "comment_count",
            "avg_top_comments_sentiment",
            "subject",
            "fetch_date",
        ]
    ]

    file_path = f"{OUTPUT_PATH}/{file_name}.csv"
    load_data_to_csv(final_df, file_path)

    return file_path
