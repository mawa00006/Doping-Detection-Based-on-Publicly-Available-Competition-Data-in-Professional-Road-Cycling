import requests as rq
from bs4 import BeautifulSoup as bs


url = 'https://en.wikipedia.org/wiki/List_of_doping_cases_in_athletics'

s = rq.Session()
response = s.get(url, timeout= 10)
