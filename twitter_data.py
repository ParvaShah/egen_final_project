import requests
from textblob import TextBlob

bearer_token = "AAAAAAAAAAAAAAAAAAAAAOJtRgEAAAAAyQrqKNQiUYJA0jqxSdaLu7nvPO8%3D3jncoxoxQRFuWPjJmZxDVrp3bjAmlYB0hFAvcrVYrF4phOUnsQ"
search_url = "https://api.twitter.com/2/tweets/search/recent"
query_params = {'query': '#aapl OR #applestock','tweet.fields': 'text,author_id,created_at'}


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def get_data():
    result = []
    response = requests.get(search_url, auth=bearer_oauth, params=query_params)
    
    if response.status_code == 200:
        json_response = response.json()
        for vals in json_response['data']:
            blob = TextBlob(vals['text'])
            result.append(blob.sentiment.polarity)
    else:
        result.append(0)
    
    return result

