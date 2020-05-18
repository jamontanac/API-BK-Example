"""
Implementation of Bluekai API adapted for python3
the idea was based in the version provided in https://docs.oracle.com/en/cloud/saas/data-cloud/data-cloud-help-center/Developers/getting_started/api_examples.html
Property of Hexagon Data
Email for support:rafael@hexagondata.io, edher@hexagondata.io alejandro@hexagondata.io
Github: hexagondata
"""
import os
import sys
import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
import http.cookiejar
import hashlib
import hmac
import base64
import json
import random
import http

class BK_API:
    headers = {"Accept":"application/json","Content-type":"application/json"}
    #--------------------------------------------------------------------------------------------------------------
    @classmethod
    def InputBuilder(cls,bkuid:str, bksecretkey:str,url:str, method:str, data:str=None)-> str:
        """
        This method construct the url needed to request the data, it make Use
        of a sha256 for encript the keys.
        * bkuid: This is the id of the user given by Oracle (type str)

        * bksecretkey: This correspond to the authenticaten private key given by Oracle (type str)

        * url: (type str) this argument correspond with the http address where we want to get our rawData
        to see more information about the required url see https://docs.oracle.com/en/cloud/saas/data-cloud/data-cloud-help-center/Developers/api_getting_started.html

        * method: (type str) it refers to the method used to request the data it can be
        Method	Description	URI
            GET (list) ---> Retrieve a collection of items.
            GET (Read) --->	Read a specific item.
            POST ---> Create a new item. (with the required JSON-formatted body)
            PUT --->	Edit an existing item. (with the required JSON-formatted body)
            DELETE --->	Delete an item.

        data: (type json, dict): All POST and PUT requests require JSON-formatted data
        in the body to create or update an object. The JSON format uses human-readable
        text to send data objects in the form of key-value pairs. The standard JSON data
        types consist of strings, integers, booleans, objects, arrays, and null values.

        Returns a string with the url parsed with its respective method and keys
        """
        stringToSign = method
        parsedUrl = urlparse(url)
        
        stringToSign += parsedUrl.path
        # splitting the query into array of parameters separated by the '&' character

        qP = parsedUrl.query.split('&')


        if len(qP) > 0:
            for  qS in qP:
                qP2 = qS.split('=', 1)

                if len(qP2) > 1:
                    stringToSign += qP2[1]


        if data != None :
            stringToSign += data


        # Encoding for hmac method
        stringToSign = stringToSign.encode("utf-8")
        h = hmac.new(bksecretkey.encode("utf-8"), stringToSign, hashlib.sha256)

        s = base64.standard_b64encode(h.digest())


        u = urllib.parse.quote(s.decode("utf-8"))


        newUrl = url
        if url.find('?') == -1 :
            newUrl += '?'
        else:
            newUrl += '&'

        newUrl += 'bkuid=' + bkuid + '&bksig=' + u

        return newUrl

    @classmethod
    def doRequest(cls,url:str, method:str, data:str=None):
        """
        This method executes the request of the data with the url parsed from the Method
        InputBuilder, if you want to see the meaning of the arguments passed in this function
        check the documentation of the function InputBuilder where the parameters are explained in Detail

        Return a list with the result of the consult if the response of the request is 200 (sucess)
        otherwise it print the message informing that it was not possible to do the Request
        """
        try:
            cJ = http.cookiejar.CookieJar()
            request = None
            if method == 'PUT':
                request = urllib.request.Request(url, data, cls.headers)
                request.get_method = lambda: 'PUT'
            elif  method == 'DELETE' :
                request = urllib.request.Request(url, data, cls.headers)
                request.get_method = lambda: 'DELETE'
            elif method == "POST":
                print("Post Method!")
                request = urllib.request.Request(url, data.encode("utf-8"), cls.headers)
                request.get_method = lambda: 'POST'
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cJ))
                u = urlopen(request)
                rawData = u.read()
                return True
            elif data != None :
                request = urllib.request.Request(url, data, cls.headers)
            else:
                request = urllib.request.Request(url, None, cls.headers)
                opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cJ))
                u = opener.open(request)
                rawData = u.read()
                return rawData.decode("UTF-8")

        except urllib.error.HTTPError as e:
            raise ConnectionError("HTTP error: %d %s \n ERROR: %s" % (e.code, str(e),e.read()))
        except urllib.error.URLError as e:
            raise ConnectionError("Network error: %s \n ERROR: %s" % (e.reason.args[1], e.read()))
