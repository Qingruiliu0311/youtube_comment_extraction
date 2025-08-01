import os
import json
import re
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union

class YouTubeCommentsKeywordExtractor:
    def __init__(self, api_key):
        """
        Initialize the YouTube Comments Keyword Extractor
        
        Args:
            api_key (str): Your YouTube Data API v3 key
        """
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def extract_video_id(self, url):
        """
        Extract video ID from YouTube URL
        
        Args:
            url (str): YouTube video URL
            
        Returns:
            str: Video ID or None if invalid URL
        """
        if 'youtube.com/watch?v=' in url:
            return url.split('v=')[1].split('&')[0]
        elif 'youtu.be/' in url:
            return url.split('youtu.be/')[1].split('?')[0]
        elif len(url) == 11:  # Direct video ID
            return url
        else:
            return None
    
    def get_date_range_iso(self, days_ago_start=None, days_ago_end=None, start_date=None, end_date=None):
        """
        Convert date range to ISO format for YouTube API
        
        Args:
            days_ago_start (int): How many days ago to start from (optional)
            days_ago_end (int): How many days ago to end at (optional)
            start_date (str): Start date in YYYY-MM-DD format (optional)
            end_date (str): End date in YYYY-MM-DD format (optional)
            
        Returns:
            tuple: (published_after, published_before) in ISO format
        """
        now = datetime.now()
        
        if start_date:
            published_after = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%dT00:00:00Z')
        elif days_ago_start:
            published_after = (now - timedelta(days=days_ago_start)).strftime('%Y-%m-%dT00:00:00Z')
        else:
            published_after = None
            
        if end_date:
            published_before = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%dT23:59:59Z')
        elif days_ago_end:
            published_before = (now - timedelta(days=days_ago_end)).strftime('%Y-%m-%dT23:59:59Z')
        else:
            published_before = None
            
        return published_after, published_before
    
    def search_videos_by_keyword(self, keyword, max_results=50, published_after=None, published_before=None):
        """
        Search for videos by keyword with date range and view count ordering
        
        Args:
            keyword (str): Search keyword for videos
            max_results (int): Maximum number of videos to find
            published_after (str): ISO format date (e.g., '2023-01-01T00:00:00Z')
            published_before (str): ISO format date (e.g., '2024-01-01T00:00:00Z')
            
        Returns:
            list: List of video information with view counts
        """
        videos = []
        next_page_token = None
        
        try:
            # First, search for videos
            while len(videos) < max_results:
                remaining = max_results - len(videos)
                batch_size = min(50, remaining)
                
                search_params = {
                    'part': 'snippet',
                    'q': keyword,
                    'type': 'video',
                    'maxResults': batch_size,
                    'order': 'relevance',  # First get relevant videos
                    'pageToken': next_page_token
                }
                
                if published_after:
                    search_params['publishedAfter'] = published_after
                if published_before:
                    search_params['publishedBefore'] = published_before
                
                request = self.youtube.search().list(**search_params)
                response = request.execute()
                
                # Get video IDs for statistics call
                video_ids = [item['id']['videoId'] for item in response['items']]
                
                # Get detailed video statistics including view count
                if video_ids:
                    stats_request = self.youtube.videos().list(
                        part='statistics,snippet',
                        id=','.join(video_ids)
                    )
                    stats_response = stats_request.execute()
                    
                    # Create a mapping of video_id to statistics
                    stats_map = {video['id']: video for video in stats_response['items']}
                    
                    for item in response['items']:
                        video_id = item['id']['videoId']
                        stats = stats_map.get(video_id, {})
                        
                        video_info = {
                            'video_id': video_id,
                            'title': item['snippet']['title'],
                            'channel': item['snippet']['channelTitle'],
                            'channel_id': item['snippet']['channelId'],
                            'published_at': item['snippet']['publishedAt'],
                            'description': item['snippet']['description'],
                            'thumbnail': item['snippet']['thumbnails']['default']['url'],
                            'view_count': int(stats.get('statistics', {}).get('viewCount', 0)),
                            'like_count': int(stats.get('statistics', {}).get('likeCount', 0)),
                            'comment_count': int(stats.get('statistics', {}).get('commentCount', 0))
                        }
                        videos.append(video_info)
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
        except HttpError as e:
            print(f"Error searching videos: {e}")
        
        # Sort by view count (descending)
        videos.sort(key=lambda x: x['view_count'], reverse=True)
        return videos
    
    def get_top_comments_by_likes(self, video_id, max_comments=10):
        """
        Get top comments from a video ordered by like count
        
        Args:
            video_id (str): YouTube video ID
            max_comments (int): Maximum number of top comments to retrieve
            
        Returns:
            list: List of top comments ordered by likes
        """
        comments = []
        next_page_token = None
        total_fetched = 0
        
        try:
            # Fetch more comments than needed to ensure we get the top ones
            fetch_limit = min(max_comments * 5, 100)  # Fetch 5x more to filter top ones
            
            while total_fetched < fetch_limit:
                remaining = fetch_limit - total_fetched
                batch_size = min(100, remaining)
                
                request = self.youtube.commentThreads().list(
                    part='snippet,replies',
                    videoId=video_id,
                    maxResults=batch_size,
                    order='relevance',  # Get most relevant comments first
                    pageToken=next_page_token
                )
                
                response = request.execute()
                
                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    
                    comment_data = {
                        'video_id': video_id,
                        'comment_id': item['snippet']['topLevelComment']['id'],
                        'author': comment['authorDisplayName'],
                        'author_channel_id': comment.get('authorChannelId', {}).get('value', 'N/A'),
                        'text': comment['textDisplay'],
                        'like_count': comment['likeCount'],
                        'published_at': comment['publishedAt'],
                        'updated_at': comment['updatedAt'],
                        'reply_count': item['snippet']['totalReplyCount']
                    }
                    comments.append(comment_data)
                
                total_fetched += len(response['items'])
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
        except HttpError as e:
            error_details = json.loads(e.content.decode())
            error_reason = error_details['error']['errors'][0]['reason']
            
            if error_reason == 'commentsDisabled':
                print(f"Comments disabled for video {video_id}")
            elif error_reason == 'videoNotFound':
                print(f"Video {video_id} not found or is private")
            else:
                print(f"Error retrieving comments for {video_id}: {e}")
        
        # Sort by like count (descending) and return top comments
        comments.sort(key=lambda x: x['like_count'], reverse=True)
        return comments[:max_comments]
    
    def extract_top_comments_from_videos(self, videos, top_comments_count=10):
        """
        Extract top comments by likes from multiple videos
        
        Args:
            videos (list): List of video information
            top_comments_count (int): Number of top comments to extract per video
            
        Returns:
            dict: Results organized by video with top comments
        """
        results = {
            'extraction_info': {
                'total_videos_processed': 0,
                'videos_with_comments': 0,
                'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'top_comments_per_video': top_comments_count
            },
            'videos': []
        }
        
        for i, video in enumerate(videos):
            print(f"Processing video {i+1}/{len(videos)}: {video['title'][:50]}...")
            
            # Get top comments by likes
            top_comments = self.get_top_comments_by_likes(video['video_id'], top_comments_count)
            
            video_result = {
                'video_info': video,
                'top_comments': top_comments,
                'comments_extracted': len(top_comments)
            }
            
            results['videos'].append(video_result)
            
            # Update statistics
            results['extraction_info']['total_videos_processed'] += 1
            if top_comments:
                results['extraction_info']['videos_with_comments'] += 1
            
            print(f"Extracted {len(top_comments)} top comments")
            
            # Rate limiting to be respectful to API
            time.sleep(0.5)
        
        return results
    
    def save_to_excel(self, results, filename=None):
        """
        Save results to Excel file in current folder
        
        Args:
            results (dict): Results from extract_top_comments_from_videos
            filename (str): Custom filename (optional)
            
        Returns:
            str: Path to saved Excel file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"youtube_top_comments_{timestamp}.xlsx"
        elif not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        # Prepare data for Excel
        excel_data = []
        
        for video in results['videos']:
            video_info = video['video_info']
            for i, comment in enumerate(video['top_comments'], 1):
                excel_data.append({
                    'Video_Rank': results['videos'].index(video) + 1,
                    'Video_ID': video_info['video_id'],
                    'Video_Title': video_info['title'],
                    'Channel_Name': video_info['channel'],
                    'Video_Views': video_info['view_count'],
                    'Video_Likes': video_info['like_count'],
                    'Video_Published': video_info['published_at'],
                    'Comment_Rank': i,
                    'Comment_ID': comment['comment_id'],
                    'Comment_Author': comment['author'],
                    'Comment_Text': comment['text'],
                    'Comment_Likes': comment['like_count'],
                    'Comment_Published': comment['published_at'],
                    'Reply_Count': comment['reply_count']
                })
        
        if excel_data:
            # Create DataFrame
            df = pd.DataFrame(excel_data)
            
            # Save to Excel with formatting
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Main data sheet
                df.to_excel(writer, sheet_name='Top_Comments', index=False)
                
                # Summary sheet
                summary_data = {
                    'Metric': [
                        'Total Videos Processed',
                        'Videos with Comments',
                        'Total Comments Extracted',
                        'Extraction Date',
                        'Comments per Video'
                    ],
                    'Value': [
                        results['extraction_info']['total_videos_processed'],
                        results['extraction_info']['videos_with_comments'],
                        len(excel_data),
                        results['extraction_info']['extraction_date'],
                        results['extraction_info']['top_comments_per_video']
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Video overview sheet
                video_overview = []
                for video in results['videos']:
                    video_info = video['video_info']
                    video_overview.append({
                        'Rank': results['videos'].index(video) + 1,
                        'Video_ID': video_info['video_id'],
                        'Title': video_info['title'],
                        'Channel': video_info['channel'],
                        'Views': video_info['view_count'],
                        'Likes': video_info['like_count'],
                        'Comments_Count': video_info['comment_count'],
                        'Published': video_info['published_at'],
                        'Comments_Extracted': video['comments_extracted']
                    })
                
                video_df = pd.DataFrame(video_overview)
                video_df.to_excel(writer, sheet_name='Video_Overview', index=False)
            
            print(f"Excel file saved: {filename}")
            print(f"Location: {os.path.abspath(filename)}")
            return filename
        else:
            print("No data to save to Excel")
            return None

def main():
    """
    Main function with updated search options
    """
    API_KEY = "AIzaSyAL7o9KuNeYjMQufCJz3xb0oOKe9mbolQA"
    
    extractor = YouTubeCommentsKeywordExtractor(API_KEY)
    
    print("YouTube Top Comments Extractor")
    print("=" * 40)
    
    # Get search keyword
    keyword = input("Enter keyword to search for videos: ").strip()
    if not keyword:
        print("No keyword provided!")
        return
    
    # Get date range options
    print("\nDate Range Options:")
    print("1. Last 7 days")
    print("2. Last 30 days")
    print("3. Last 3 months")
    print("4. Last 6 months")
    print("5. Last year")
    print("6. Custom date range")
    print("7. No date filter (all time)")
    
    date_choice = input("Choose date range (1-7): ").strip()
    
    published_after = None
    published_before = None
    
    if date_choice == "1":
        published_after, _ = extractor.get_date_range_iso(days_ago_start=7)
    elif date_choice == "2":
        published_after, _ = extractor.get_date_range_iso(days_ago_start=30)
    elif date_choice == "3":
        published_after, _ = extractor.get_date_range_iso(days_ago_start=90)
    elif date_choice == "4":
        published_after, _ = extractor.get_date_range_iso(days_ago_start=180)
    elif date_choice == "5":
        published_after, _ = extractor.get_date_range_iso(days_ago_start=365)
    elif date_choice == "6":
        start_date = input("Enter start date (YYYY-MM-DD): ").strip()
        end_date = input("Enter end date (YYYY-MM-DD, or press Enter for today): ").strip()
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        published_after, published_before = extractor.get_date_range_iso(
            start_date=start_date, end_date=end_date
        )
    
    # Get number of videos to process
    max_videos = int(input("Maximum videos to process (default 10): ") or "10")
    
    # Get number of top comments per video
    top_comments_count = int(input("Top comments per video (default 10): ") or "10")
    
    print(f"\nSearching for videos with keyword: '{keyword}'")
    if published_after:
        print(f"Date range: from {published_after[:10]} to {published_before[:10] if published_before else 'today'}")
    print(f"Will process top {max_videos} videos by view count")
    print(f"Extracting top {top_comments_count} comments per video")
    
    # Search for videos
    print("\nSearching for videos...")
    videos = extractor.search_videos_by_keyword(
        keyword, max_videos, published_after, published_before
    )
    
    if not videos:
        print("No videos found with the specified criteria!")
        return
    
    print(f"Found {len(videos)} videos, ordered by view count:")
    for i, video in enumerate(videos[:5], 1):  # Show top 5
        print(f"{i}. {video['title'][:60]}... ({video['view_count']:,} views)")
    
    if len(videos) > 5:
        print(f"... and {len(videos) - 5} more videos")
    
    # Extract top comments
    print(f"\nExtracting top {top_comments_count} comments from each video...")
    results = extractor.extract_top_comments_from_videos(videos, top_comments_count)
    
    # Display summary
    print("\n" + "="*50)
    print("EXTRACTION COMPLETE")
    print("="*50)
    info = results['extraction_info']
    print(f"Videos processed: {info['total_videos_processed']}")
    print(f"Videos with comments: {info['videos_with_comments']}")
    total_comments = sum(len(v['top_comments']) for v in results['videos'])
    print(f"Total comments extracted: {total_comments}")
    
    if total_comments > 0:
        # Save to Excel
        custom_filename = input("\nEnter custom filename (or press Enter for auto-generated): ").strip()
        filename = extractor.save_to_excel(results, custom_filename)
        
        if filename:
            print(f"\n✅ Excel file created successfully!")
            print(f"📁 File location: {os.path.abspath(filename)}")
            print(f"📊 The file contains 3 sheets:")
            print("   - Top_Comments: All extracted comments")
            print("   - Video_Overview: Summary of all videos")
            print("   - Summary: Extraction statistics")
    else:
        print("No comments were extracted.")

if __name__ == "__main__":
    main()