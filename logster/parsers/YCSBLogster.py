import time
import re
import optparse
import json

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException

class YCSBLogster(LogsterParser):
    
    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.metrics = {}

        if option_string:
            options = option_string.split(' ')
        else:
            options = []
        
        optparser = optparse.OptionParser()
        
        opts, args = optparser.parse_args(args=options)
            
        self.levels = ['INFO']
        
        for level in self.levels:
            # Track counts from 0 for each log level
            setattr(self, level, 0)
        
        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line (in this case, a log level such as WARN, ERROR, or FATAL).
        self.reg = re.compile('[0-9-_:\.]+ (?P<log_level>%s)' % ('|'.join(self.levels)) )
        
        
    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''
        
        try:
            # Apply regular expression to each line and extract interesting bits.
            regMatch = self.reg.match(line)
            
            if regMatch:
                linebits = regMatch.groupdict()
                log_level = linebits['log_level']
                
                if log_level in self.levels:
                    current_val = getattr(self, log_level)
                    setattr(self, log_level, current_val+1)
                    self.parse_json_line(self, line[6:]) # all except INFO

            else:
                raise LogsterParsingException("regmatch failed to match")
                
        except Exception as e:
            raise LogsterParsingException("regmatch or contents failed with %s" % e)
            
    def parse_json_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        try:
            json_data = json.loads(line)
        except Exception as e:
            raise LogsterParsingException("{0} - {1}".format(type(e), e))
        self.metrics = self.flatten_object(json.loads(line), self.key_separator, self.key_filter)



    def key_filter(self, key):
        '''
        Default key_filter method.  Override and implement
        this method if you want to do any filtering or transforming
        on specific keys in your JSON object.
        '''
        return key

    def flatten_object(self, node, separator='.', key_filter_callback=None, parent_keys=[]):
        """
        Recurses through dicts and/or lists and flattens them
        into a single level dict of key: value pairs.  Each
        key consists of all of the recursed keys joined by
        separator.  If key_filter_callback is callable,
        it will be called with each key.  It should return
        either a new key which will be used in the final full
        key string, or False, which will indicate that this
        key and its value should be skipped.
        """
        items = {}

        try:
            if sys.version_info >= (3, 0):
                iterator = iter(node.items())
            else:
                iterator = node.iteritems()
        except AttributeError:
            iterator = enumerate(node)

        for key, item in iterator:
            # If key_filter_callback was provided,
            # then call it on the key.  If the returned
            # key is false, then, we know to skip it.
            if callable(key_filter_callback):
                key = key_filter_callback(key)
            if key is False:
                continue;

            if type(item) in (list, dict):
                # merge the items all together
                items.update(self.flatten_object(item, separator, key_filter_callback, parent_keys + [str(key)]))
            else:
                final_key = separator.join(parent_keys + [str(key)])
                items[final_key] = item

        return items

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        self.duration = duration

        metric_objects = []
        for metric_name, metric_value in self.metrics.items():
            if type(metric_value) == float:
                metric_type = 'float'
            elif type(metric_value)  == int or type(metric_value) == long:
                metric_type = 'int32'
            else:
                metric_type = 'string'
                metric_value = str(metric_value)

            metric_objects.append(MetricObject(metric_name, metric_value, type='int'))

        return metric_objects
