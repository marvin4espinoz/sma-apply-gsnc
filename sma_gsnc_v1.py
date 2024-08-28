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
base_url = 'https://gsnc.smapply.org/api/programs/'

client_id = 'XMiwNcm08erf2w0hUqu7Gy42CyoAFyrrsfxEAz0o'                                                                                               #client id
client_secret = 'uXkxdVF42G0Yd6t4ssXgxyj1oBIOGbzVRRia9rYxQIoIqWXER3Wz6gOCGQJQmkvgXgOIC58KuLiUzDLxuMNtLhKDLv5VbAImQ7gfw6af5AOAxvfVzqiLr2ds81DuDQxz'    #client secret
access_token = '97f90b6b9aacbd5aecc82794121530619b40de93'   #access token
refresh_token ='23fcac538298105d29b1de6a40eed8fdf0fbb70c'
scope = 'admin'


