# File:        PleaseRespond.py
# Author:      Lucas Smallcomb
# Email:       lucas.smallcomb@gmail.com
# Description: An HTTP datastream analysis program for Enlighten IT Consulting. This 
#              program is a solution to the "Please Respond" coding prompt found at 
#              https://gitlab.com/enlighten-challenge/please-respond
# Input:       HTTP data found at https://www.meetup.com/UXSpeakeasy/events/258247836/
# Output:      Console output of definition {total, future_date, future_url, co_1, 
#              co_1_count, co_2, co_2_count, co_3, co_3_count}
# Dev Info:    Python 3.7, Anaconda 3 distribution (PIP built-in)
# Runtime:     60.4485 seconds (with 'STREAM_TIME = 60')

from collections import Counter as ctr
from datetime import datetime as dtm
import requests as rqs
import time
import json

# The 'STREAM_TIME' integer may be modified to change the duration of the 
# datastream (default of 60 seconds)
STREAM_TIME  = 60
GROUP_KEY    = 'group'
GROUP_CO_KEY = 'group_country'
EVENT_KEY    = 'event'
TIME_KEY     = 'time'
URL_KEY      = 'event_url'

def main():
    
    total        = 0
    maxDate      = 0.0
    maxURL       = ""
    countryFreq  = []

    # Establishes HTTP stream connection, creates data object for iteration
    HTTP_Connection = rqs.get('http://stream.meetup.com/2/rsvps', stream = True)
    streamData      = HTTP_Connection.iter_lines()

    stopTime = time.time() + STREAM_TIME

    # Iterates over datastream
    for line in streamData:
    
        # Converts data of type 'byte' to 'dict' using JSON formatting
        lineDict = json.loads(line)
        total   += 1
        # Constructs frequency list of countries
        countryFreq.append(lineDict[GROUP_KEY][GROUP_CO_KEY])
    
        # Calculates 3 most frequently occurring countries
        topThree = ctr(countryFreq)
        topThree = topThree.most_common(3)
    
        # Finds the maximum date and its corresponding URL
        if lineDict[EVENT_KEY][TIME_KEY] > maxDate:
            maxDate = lineDict[EVENT_KEY][TIME_KEY]
            maxURL  = lineDict[EVENT_KEY][URL_KEY]
    
        # Time limit of stream
        if time.time() >= stopTime:
            break

    # Converts UNIX timestamp to standard date/time
    maxDate = (dtm.utcfromtimestamp(maxDate / 1000).strftime('%Y-%m-%d %H:%M:%S'))
   
    print(total, maxDate, maxURL, topThree[0][0], topThree[0][1], topThree[1][0], 
    topThree[1][1], topThree[2][0], topThree[2][1], sep = ',')
    
main()
