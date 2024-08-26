#script to grab application data straight from SMA backend database using REST API

#Step #0: Installing proper packages for script work
import requests
import numpy
import pandas as pd
import csv
from requests_oauthlib import OAuth1
import urllib
import json
import os.path
import seaborn as sb
import matplotlib.pyplot as plt
from itertools import cycle, islice
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import pyplot as plot


#Set up authorizations
#Step #1: Get Access to Illuminate Files
#1.1: Set up required access information
base_url = 'https://kippindy.illuminateed.com/live/rest_server.php'

consumer_key = 'efed90c6e89ce893f809d881ad0f3f16940b4c75'
consumer_secret = '9cc3eb42191e140a4d31e83c26460142afac91b4'

user_key = '310DC596E18B'
user_secret ='51e1988ef3854c5ecc184adfd01be6ade6435e51'


