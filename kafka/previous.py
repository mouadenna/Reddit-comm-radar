import praw
import datetime
import time
import requests

def extract_comments_from_json(comments_data):
    all_comment_texts = []

    def extract_comment_text(comment):
        if comment.get('author') == 'AutoModerator':
            return
        if 'body' in comment and comment['body']:
            all_comment_texts.append(comment['body'])
        replies = comment.get('replies', {})
        if isinstance(replies, dict):
            replies_children = replies.get('data', {}).get('children', [])
            for reply in replies_children:
                if reply.get('kind') == 't1':
                    extract_comment_text(reply.get('data', {}))

    for child in comments_data.get('data', {}).get('children', []):
        if child.get('kind') == 't1':
            extract_comment_text(child.get('data', {}))

    return all_comment_texts

def fetch_comments(post_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Reddit Historical Scraper'
    }
    json_url = post_url.rstrip('/') + '.json'
    try:
        time.sleep(2)  # polite delay
        response = requests.get(json_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list) and len(data) > 1:
            return extract_comments_from_json(data[1])
        return []
    except Exception as e:
        print(f"Error fetching comments from {post_url}: {str(e)}")
        return []

def fetch_reddit_subreddit_last_24h(subreddit_name, limit=None):
    """
    Fetch posts + comments from a specific subreddit from the last 24 hours.

    Returns a list of dicts with post and comments info.
    """
    reddit = praw.Reddit(
        client_id="WE9cMa51atcAuT5vk82o4w",
        client_secret="gKSbsR83IF45C2jruOi6gsCQ8fyLPw",
        user_agent="reddit-subreddit-historical-scraper"
    )

    now = time.time()
    after_epoch = now - 24 * 60 * 60  # 24 hours ago

    posts_data = []
    subreddit = reddit.subreddit(subreddit_name)
    count = 0

    # PRAW's subreddit.new() returns posts sorted newest to oldest; we will filter by date
    for submission in subreddit.new(limit=limit):
        created_time = submission.created_utc
        if created_time < after_epoch:
            # Since posts are newest to oldest, we can break early
            break

        post_text = submission.title
        if submission.is_self and submission.selftext:
            post_text += "\n\n" + submission.selftext

        permalink = f"https://www.reddit.com{submission.permalink}"
        comments = fetch_comments(permalink)

        post_info = {
            "id": submission.id,
            "subreddit": submission.subreddit.display_name,
            "created_utc": datetime.datetime.fromtimestamp(created_time).strftime("%Y-%m-%d %H:%M:%S"),
            "post_text": post_text,
            "comments": comments,
            "permalink": permalink,
            "score": submission.score,
            "num_comments": submission.num_comments,
            "author": str(submission.author)
        }

        posts_data.append(post_info)
        count += 1
        print(f"[{count}] Fetched post {submission.id} with {len(comments)} comments")

    print(f"Total posts fetched: {count}")
    return posts_data


if __name__ == "__main__":
    posts_data = fetch_reddit_subreddit_last_24h("Morocco")
    print(len(posts_data))