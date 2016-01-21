#!/usr/bin/python

try:
    from config import config as sensConfig
except:
    raise IOError("No configuration present. Please copy config/config.py to the current folder and edit it.")

# If we're storing the data in MongoDB import the mongoDB library.
if sensConfig['mongoStore']:
    import pymongo

import threading
import traceback
import httplib
import time
import json
import sensLog
import Queue
import datetime
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from SocketServer import ThreadingMixIn
from urlparse import urlparse
from pprint import pprint


###########
# CLASSES #
###########

class dataHandler(threading.Thread):
    def __init__(self, logger, queue):
        """
        dataHandler class handles incoming data from sources.
        Accepts two arguments: a sensLog instance and a Queue instance.
        """
        
        logger.log("Init dataHandler.")
        
        # Do the thread init thing!
        threading.Thread.__init__(self)
        
        # If we're configured to store things in MongoDB...
        if sensConfig['mongoStore']:
            try:
                # Mongo server and DB.
                self.__mongo = pymongo.MongoClient(sensConfig['mongoHost'], sensConfig['mongoPort'])
                self.__mDB = self.__mongo[sensConfig['mongoDBName']]
                
                # Mongo collections.
                self.__mCollReading = self.__mDB[sensConfig['readingColl']]
                self.__mCollsensor = self.__mDB[sensConfig['sensorColl']]
            
            except Exception as e:
                tb = traceback.format_exc()
                logger.log("Failed to create mongoDB objects:\n%s" %tb)
                
                # Pass the exception up the chain to prevent us from starting.
                raise e
        
        # Set up logger.
        self.__logger = logger
        
        # Set up queue.
        self.__queue = queue
        
        # Keep running?
        self.__keepRunning = True
        
        # Keep a record of all sensor readings.
        self.__sensorReadings = {}
        
        # Keep a record of all sensors.
        self.__sensors = {}
    
    # Convert datetime objects expressed as a string back to datetime
    def __toDatetime(self, strDateTime):
        """
        Convert utcnow() datetime string back to a datetime object.
        """
        
        # Do some extra logic hee to make sure the timestamps are correct before conversion.
        if (len(strDateTime) == 19):
            # Appent microsceconds when on an exact second.
            strDateTime = strDateTime + ".000000"
        
        try:
            # Attmpet to convert.
            retVal = datetime.datetime.strptime(strDateTime, "%Y-%m-%d %H:%M:%S.%f")
        
        except Exception as e:
            raise e
        
        return retVal
    
    def __filterHandler(self, sensName):
        """
        Check for filters against a given sensor, and execute filter actions.
        """
        
        # Not yet implemented.
        
        return
    
    def __worker(self):
        """
        Primary data worker.
        """
        self.__logger.log("Staring dataHandler worker.")
        
        while self.__keepRunning:
            try:
                # Grab the JSON put on the queue
                newData = self.__queue.get()
                
                # If we're debugging log the things.
                if sensConfig['debug']:
                    self.__logger.log("dequeue: %s" %newData)
                
                # Hold data temporarily until we have everything in the latest dataset
                # to ensure the update to the master sensor readings are atomic.
                tempData = {}
                
                for entry in newData:
                    # Add the data to our tempData
                    tempData.update({entry: newData[entry]})
                    
                    # Correct timestamp datatype.
                    tsStr = tempData[entry].pop('time')
                    goodTs = self.__toDatetime(tsStr)
                    
                    # Set the new 'time' with a proper date object.
                    tempData[entry]['time'] = goodTs
                    
                    # And store the data if we're configured to.
                    if sensConfig['mongoStore']:
                        try:
                            # Add the sensor ID to the data.
                            thisInsert = tempData[entry]
                            thisInsert.update({'sensorID': entry})
                            
                            # Now upload to mongoDB.
                            self.__mCollReading.insert(thisInsert)
                        
                        except:
                            tb = traceback.format_exc()
                            self.__logger.log("Failed to insert data into MongoDB:\n%s" %tb)
                    
                    try:
                        # Consult filters.
                        self.__filterHandler(entry)
                    
                    except:
                        # If the filter handler pukes just log an exception.
                        tb = traceback.format_exc()
                        self.__logger.log("Filter handler choked on %s:\n%s" %(entry, tb))
                
                # Update master sensor readings.
                self.__sensorReadings.update(tempData)
                
            except:
                tb = traceback.format_exc()
                self.__logger.log("Client queue worker caught exception reading from queue:\n%s" %tb)
    
    def getLastReadings(self):
        """
        Get the last readings from all sensors that are submitting readings.
        """
        
        return self.__sensorReadings
    
    def getSensorList(self):
        """
        Get a list of all sensors and their metadata.
        """
        
        return self.__sensors
    
    def run(self):
        """
        Start the worker.
        """
        
        # Call the main worker.
        self.__worker()


class httpDataLayer(threading.Thread):
    def __init__(self, name, logger, queue):
        """
        httpDataLayer supports grabbing data from a sensorNet data source.
        Accepts three variables: the name name of the HTTP data layer, an instance of sensLog that will be used for logging, and a Queue object.
        """
        logger.log("Init thread for %s." %name)
        threading.Thread.__init__(self)
        
        # My name...
        self.__myName = name
        
        # Logger.
        self.__logger = logger
        
        # Config params
        self.__myConfig = sensConfig['endpoints'][self.__myName]
        
        # Client queue.
        self.__queue = queue
        
        # Try to vaidate the URL we've been given and set necessary parameters.
        try:
            # Break the URL into parts we need for the code.
            urlParts = urlparse(self.__myConfig['baseURL'])
        
        except Exception as e:
            raise e
        
        # We only support HTTP.
        if urlParts.scheme == 'http':
            # Get the necessary parts of the URL to do what we need to do.
            self.__targetPort = urlParts.port
            self.__targetHost = urlParts.hostname
            self.__targetPath = urlParts.path
        
        else:
            # Raise a runtime exception.
            raise RuntimeError("htpDataLayer only supports HTTP endpoints.")
        
        # Keep running by default.
        self.__keepRunning = True

    def __parseResp(self, resp):
        """
        Parse and process responses from the HTTP client.
        """
        
        # Set up return value and a dictionary to store incoming data.
        retVal = {}
        initialData = None
        
        try:
            # Get our JSON data.
            initialData = json.loads(resp)
        
        except:
            tb = traceback.format_exc()
            self.__logger.log("Caught exception loading JSON data:\n%s" %tb)
        
        # Add metadata to our sensor output.
        for sensor in initialData:
            # Tack the data source into the response info.
            initialData[sensor].update({'dataSrc': self.__myName})
            # Add the respnose info to the return value.
            retVal.update({sensor: initialData[sensor]})
        
        return retVal

    def __worker(self):
        """
        httpDataLayer worker.
        """
        
        logger.log("Staring %s worker." %self.__myName)
        
        # Flag the connection as not being connected.
        notConnected = True
        connected = False
        
        # Keep running until we don't.
        while self.__keepRunning:
            # While we're not connected keep trying to connect.
            while notConnected:
                try:
                    # Build the connection.
                    conn = httplib.HTTPConnection(self.__targetHost, self.__targetPort)
                    
                    # Set flags
                    connected = True
                    notConnected = False
                    
                except:
                    tb = traceback.format_exc()
                    self.__logger.log("%s" %tb)
            
            while connected:
                try:
                    # Do a request.
                    conn.request("GET", self.__targetPath + "thermal/")
                
                except Exception as e:
                    # Flag us as disconnected.
                    connected = False
                    notConnected = True
                    
                    tb = traceback.format_exc()
                    self.__logger.log("Got exception making HTTP request:\n%s" %tb)
                
                # Grab the response.
                resp = conn.getresponse()
                
                # If we have a 200, let's keep going.
                if resp.status == 200:
                    try:
                        # Get the JSON data.
                        respData = resp.read()
                        
                        # Debug?
                        if sensConfig['debug']:
                            self.__logger.log("%s enqueue: %s" %(self.__myName, respData.strip()))
                        
                        try:
                            # Parse our response and add some metadata.
                            parsedResp = self.__parseResp(respData)
                            
                            # Enqueue the things.
                            self.__queue.put(parsedResp)
                        
                        except Exception as e:
                            tb = traceback.format_exc()
                            self.__logger.log("Failed ot parse and enqueue data:\n%s" %tb)
                    
                    except Exception as e:
                        tb = traceback.format_exc()
                        self.__logger.log("Got exception reading HTTP respnose data:\n%s" %tb)
                        
                        # Flag us as disconnected.
                        connected = False
                        notConnected = True
                        
                        raise e
                
                else:
                    self.__logger.log("HTTP status: %s" %resp.status)
                
                # Wait before the next request.
                time.sleep(self.__myConfig['pollInterval'])
            
            # Shut down the connection.
            self.__logger.log("%s disconnecting." %self.__myName)
            conn.close()
    
    def run(self):
        """
        Start the worker.
        """
        
        self.__worker()



#####################
# WEB SERVER ENGINE #
#####################

# Override the HTTPRequestHandler
#class HTTPRequestHandler(BaseHTTPRequestHandler):
#    # Do nothing if we get POST data.
#    def do_POST(self):
#        pass
#
#    # Handle GETs.
#    def do_GET(self):
#        # Hold the data we want to try sending.
#        sendData = None
#        
#        try:
#            # If we request the proper thing send it.
#            if None != re.search('^/v1/readings(/)?(.+)?$', self.path):
#                
#                # Set newData to None by default.
#                newData = None
#                
#                # Get match chunks.
#                chunks = re.match('^/v1/readings(/)?(.+)?$', self.path)
#                
#                # If we have a URL sans slash...
#                if (chunks.groups()[0] == None) and (chunks.groups()[1] == None):
#                    # Set our HTTP status stuff.
#                    httpStatus = 200
#                    sendData = json.dumps(thermalNet.getReadings()) + "\n"
#                    
#                elif (chunks.groups()[0] == "/") and (chunks.groups()[1] == None):
#                    # Set our HTTP status stuff.
#                    httpStatus = 200
#                    sendData = json.dumps(thermalNet.getReadings()) + "\n"
#                
#                elif (chunks.groups()[0] == "/") and (chunks.groups()[1] != None):
#                    # Grab the target sensor
#                    targetSensor = chunks.groups()[1]
#                    
#                    try:
#                        # Get the new data from the end of the URL.
#                        
#                        ### FIX ME!!!
#                        newData = {targetSensor: thermalNet.getReadings()[targetSensor]}
#                    
#                    except KeyError:
#                        # Set HTTP 404.
#                        httpStatus = 404
#                        sendData = None
#                    
#                    except Exception as e:
#                        # Pass other exceptions back up.
#                        raise e
#                    
#                    # If we got some new data...
#                    if (newData != None) and (newData != {}):
#                        # Set our HTTP status stuff.
#                        httpStatus = 200
#                        sendData = json.dumps(newData) + "\n"
#                    
#                    else:
#                        # Set HTTP 404.
#                        httpStatus = 404
#                        sendData = None
#                
#                else:
#                    # 404, no data.
#                    httpStatus = 404
#                    sendData = None
#            
#            # If we request the proper thing send it.
#            elif None != re.search('^/v1/sensors(/)?(.+)?$', self.path):
#                
#                # Get match chunks.
#                chunks = re.match('^/v1/sensors(/)?(.+)?$', self.path)
#                
#                # If we have a URL sans slash...
#                if (chunks.groups()[0] == None) and (chunks.groups()[1] == None):
#                    # Set our HTTP status stuff.
#                    httpStatus = 200
#                    sendData = json.dumps(thermalNet.getSensorMeta()) + "\n"
#                    
#                elif (chunks.groups()[0] == "/") and (chunks.groups()[1] == None):
#                    # Set our HTTP status stuff.
#                    httpStatus = 200
#                    
#                    ### FIX ME!!!
#                    sendData = json.dumps(thermalNet.getSensorMeta()) + "\n"
#                
#                elif (chunks.groups()[0] == "/") and (chunks.groups()[1] != None):
#                    
#                    # Get the new data from the end of the URL.
#                    newData = thermalNet.getSensorMeta(chunks.groups()[1])
#                    
#                    # If we got some new data...
#                    if (newData != None) and (newData != {}):
#                        # Set our HTTP status stuff.
#                        httpStatus = 200
#                        sendData = json.dumps(newData) + "\n"
#                    
#                    else:
#                        # Set HTTP 404.
#                        httpStatus = 404
#                        sendData = None
#                
#                else:
#                    # 404, no data.
#                    httpStatus = 404
#                    sendData = None
#            
#            else:
#                # 404, no data.
#                httpStatus = 404
#                sendData = None
#        
#        except KeyboardInterrupt:
#            # Pass it up.
#            raise KeyboardInterrupt
#        
#        except:
#            # HTTP 500.
#            httpStatus = 500
#            sendData = None
#            
#            tb = traceback.format_exc()
#            logger.log("Caught exception in do_get():\n%s" %tb)
#        
#        try:
#            # Send the HTTP respnose code.
#            self.send_response(httpStatus)
#            self.send_header('Content-Type', 'application/json')
#            self.send_header('Access-Control-Allow-Origin', '*')
#            self.end_headers()
#            
#            # If we have data.
#            if sendData != None:
#                # Send the data.
#                self.wfile.write(sendData)
#        
#        except:
#            tb = traceback.format_exc()
#            logger.log("Caught exception trying to send HTTP response:\n%s" %tb)
#        
#        return
#    
#    # Override logging.
#    def log_message(self, format, *args):
#        # If we're debugging or the status isn't 200 log the request.
#        if snConfig['debug'] or (args[1] != '200'): 
#            logger.log("HTTP request: [%s] %s" %(self.client_address[0], format%args))
#
## Override the ThreadedHTTPServer
#class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
#    allow_reuse_address = True
#    
#    def shutdown(self):
#        # Close the socket and shut the server down.
#        self.socket.close()
#        HTTPServer.shutdown(self)
#    
## Override the SimpleHTTPServer
#class SimpleHttpServer():
#    def __init__(self, ip, port):
#        try:
#            # Start a server.
#            self.server = ThreadedHTTPServer((ip, port), HTTPRequestHandler)
#        
#        except KeyboardInterrupt:
#            # Pass it up.
#            raise KeyboardInterrupt
#        
#        except:
#            tb = traceback.format_exc()
#            logger.log("Exception in Smiple HTTP server:\n%s" %tb)
#    
#    def start(self):
#        try:
#            logger.log('Start web server.')
#            
#            # Start the server thread.
#            self.server_thread = threading.Thread(target=self.server.serve_forever)
#            self.server_thread.daemon = True
#            self.server_thread.start()
#            
#            logger.log('Start sensor monitor.')
#            
#            # Start the server thread.
#            self.tnThread = threading.Thread(target=thermalNet.run(snConfig['sensorMode']))
#            self.tnThread.daemon = True
#            self.tnThread.start()
#        
#        except KeyboardInterrupt:
#            # Pass it up.
#            raise KeyboardInterrupt
#        
#        except:
#            tb = traceback.format_exc()
#            logger.log("Exception thrown in start()\n%s" %tb)
#            
#            # Shut down.
#            self.stop()
#        
#    def waitForThread(self):
#        try:
#            # Join the thread.
#            self.server_thread.join()
#        
#        except KeyboardInterrupt:
#            # Pass it up.
#            raise KeyboardInterrupt
#        
#        except:
#            tb = traceback.format_exc()
#            logger.log("Exception in waithForThread():\n%s" %tb)
#    
#    def stop(self):
#        # Stop the server and wait for the threads to die.
#        self.server.shutdown()
#        self.waitForThread()


#######################
# MAIN EXECUTION BODY #
#######################

if __name__ == '__main__':
    # Create the shared logger.
    logger = sensLog.sensLog(sensConfig['logMode'])

    logger.log("Sensor service starting.")

    # Flag the data worker as not started.
    dataWorkerStarted = False
    
    # Create the shared queue.
    clientQ = Queue.Queue()
    
    # Set up empty thread list.
    threadList = []
    
    # See if we have any data sources listed...
    if len(sensConfig['endpoints']) < 1:
        # If not, complain and die.
        logger.log("No endpoints defined in config.py. Exiting.")
        quit()
    
    # Start the data handler.
    logger.log("Spinning up the data handler...")
    dh = dataHandler(logger, clientQ)
    dh.daemon = True
    dh.start()
    
    # Just pretend we started the data worker.
    dataWorkerStarted = True
    
    try:
        if dataWorkerStarted:
            # Start each endpoint in its own thread.
            for endpoint in sensConfig['endpoints']:
                # Spin up our client threads.
                logger.log("Spinning up thread for endpoint %s." %endpoint)
                thisEndpoint = httpDataLayer(endpoint, logger, clientQ)
                thisEndpoint.daemon = True
                thisEndpoint.start()
                
                # Append the thread to the list.
                threadList.append(thisEndpoint)
            
            try:
                # Fix bug that prevents keyboard interrupt from killing the program.
                while True: time.sleep(10)
            
            except KeyboardInterrupt:
                logger.log("Caught keybaord interrupt. Shutting down.")
            
            except SystemExit:
                logger.log("System exit. Shutting down.")
        
        else:
            logger.log("Data handler not started. Shutting down.")
    
    except:
        tb = traceback.format_exc()
        logger.log("Exception in client threads:\n%s" %tb)
    
    logger.log("Exiting.")    