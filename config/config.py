# Configuration
config = {
    # Global configuration options.
    'logMode': 'stdout', # Set the default logging mode to standard out. You can choose 'stdout', 'syslog', or 'none'.
    'debug': False, # To debug or not to debug?
    
    # Web server config.
    'wsListenIP': '0.0.0.0', # Web server listen IP.
    'wsListenPort': 8093, # Web server listen port.
    
    # MongoDB stuff.
    'mongoStore': True, # Store data in MongoDB?
    'mongoHost': '127.0.0.1', # Hostname or IP of mongoDB server
    'mongoPort': 27017, # Port number monogoDB instance uses. The default port is 27017.
    'mongoDBName': 'sensorNet', # Name of the mongoDB our data gets put in.
    'readingColl': 'readings', # Name of the collection that holds reading data.
    'sensorColl': 'sensors', # Name of the collection that holds our sensor data.
    
    # Endpoints we pull data from.
    'endpoints': { # Mandatory: URL information for a sensorNet instance.
        #'<endpoint name>': {'baseURL': '<http://host:ports/v1/>', 'pollInterval': <seconds>}
        # Example: 'someMachine': {'baseURL': '<http://127.0.0.1:8092/v1/>', 'pollInterval': 60.0}
    },
    
    # Data filters and actions.
    'filters': { # Optional: Actions to be performed when incoming data matches a pattern.
    }
}
