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
import multiprocessing
import numpy as np
import pandas as pd
from functools import partial
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
        #print (parsedUrl)
        stringToSign += parsedUrl.path
        # splitting the query into array of parameters separated by the '&' character
        #print parsedUrl.query
        qP = parsedUrl.query.split('&')
        #print qP

        if len(qP) > 0:
            for  qS in qP:
                qP2 = qS.split('=', 1)
                #print qP2
                if len(qP2) > 1:
                    stringToSign += qP2[1]

        #print stringToSign
        if data != None :
            stringToSign += data
        #print ("\nString to be Signed:\n" + stringToSign)

        # Encoding for hmac method
        stringToSign = stringToSign.encode("utf-8")
        h = hmac.new(bksecretkey.encode("utf-8"), stringToSign, hashlib.sha256)

        s = base64.standard_b64encode(h.digest())
        #print ("\nRaw Method Signature:\n" + s.decode("utf-8") )

        u = urllib.parse.quote(s.decode("utf-8"))
        #print ("\nURL Encoded Method Signature (bksig):\n" + u)

        newUrl = url
        if url.find('?') == -1 :
            newUrl += '?'
        else:
            newUrl += '&'

        newUrl += 'bkuid=' + bkuid + '&bksig=' + u
        #print("Signed URL: "+newUrl)
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
            #print ("\nHTTP error: %d %s" % (e.code, str(e)) )
            #print ("ERROR: ", e.read())
            #return "{}"
        except urllib.error.URLError as e:
            raise ConnectionError("Network error: %s \n ERROR: %s" % (e.reason.args[1], e.read()))
            #print ("Network error: %s" % e.reason.args[1])
            #print ("ERROR: ", e.read())
            #return "{}"

class BKAudienceCall(BK_API):
    @classmethod
    def BKAudienceList(cls,pid:str,bkuid:str, bksecretkey:str)-> dict:
        """
        This function is used to consult the complete list of all Audience asociated with
        certain pid.
        Here in this function it is not necessary to introduce an Url since we take
        automatically the url correspondent to the API method.
        The result is a Dictionary (JSON type)
        """
        Url = "https://services.bluekai.com/Services/WS/audiences?pid="+pid
        signedUrl = cls.InputBuilder(bkuid=bkuid, bksecretkey=bksecretkey, url=Url, method="GET", data=None)
        Request = cls.doRequest(url=signedUrl, method="GET", data=None)
        r1 = json.loads(Request)
        return r1
    @classmethod
    def BKAudienceInfo(cls,pid:str,audienceID:str, bkuid:str, bksecretkey:str)-> dict:
        """
        This method is used to consult a specific information related to an audienceID
        If you do not have the audienceID it is not possible to use this method.
        The result is a Dictionary (JSON type)
        """
        Url = "https://services.bluekai.com/Services/WS/audiences/"+audienceID+"?pid="+pid
        signedUrl = cls.InputBuilder(bkuid, bksecretkey, Url, "GET", None)
        Request = cls.doRequest(url=signedUrl, method="GET", data=None)
        r1 = json.loads(Request)
        return r1
    @classmethod
    def Consult_Single_Audience(cls, audienceID:str, pid:str, bkuid:str, bksecretkey:str)-> list:
        """
        The purpose of this method is to simply parallelize the requests via the BK API
        Here the important thing about this function is that we take only the fields correspondent
        to  'name', 'reach', 'price', 'status'. Here we let this explicit to be modified when ever
        we want.
        Note: It is important to note that taking more fields does not affect the performance of the code,
        so don't worry about taking more fields from the request.
        """
        try:
            d=cls.BKAudienceInfo(pid=pid, audienceID=audienceID, bkuid=bkuid,bksecretkey= bksecretkey)
            return [d["name"],d['reach'],d['price'],d['status']]
        except:
            return ["Error",0,0.0,""]

    @classmethod
    def GetData_Parallel(cls,pid:str, bkuid:str, bksecretkey:str,List_ids_Audiences:list ,Processors:int = multiprocessing.cpu_count())-> pd.DataFrame:
        """
        This method is designed simply to process in parallel the multiple request. since a partner could have a lot of
        audiences it is important to consult them very quickly. So far this is an easy to tackle the problem and quite fast
        The default value of the consult is the number of cores in the machine used.
        For my machine (8 cores) it takes around 1 min 20 seconds to consult 609 audiences.
        The speed-up is approximatelly of X (number of cores -1)
        """
        with multiprocessing.Pool(Processors) as p:
            Partial_function = partial(cls.Consult_Single_Audience,pid=pid,bkuid=bkuid,bksecretkey= bksecretkey)
            request=np.array(p.map(Partial_function,List_ids_Audiences))
        df=pd.DataFrame(data=request,columns=["Name","Reach","Price","Status"])
        return df
# a=BKAudienceCall()
# bkuid = '72c487d4f0318706ce96a0ebe47b906342cb0a2e3610fe182746602c3be6d016' #Web Service User Key
# bksecretkey = '90388366ed94cfaea8ee163e0c64200a587974ce7492ec827b33e84ba644bad0'#Web Service Private Key
# print(a.BKAudienceList(pid="4355",bkuid=bkuid,bksecretkey=bksecretkey))
