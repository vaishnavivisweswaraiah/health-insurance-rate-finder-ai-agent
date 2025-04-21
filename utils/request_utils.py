import requests

def make_api_call(url):
    respone = requests.get(url)
    return respone