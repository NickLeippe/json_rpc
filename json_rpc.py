##############################################################################
#
# Python Json-RPC Service handler
#
# (C) 2012 Domino Logic, LLC
# Author: Nicholas Leippe
#
# Description:
# - handles both Json-RPC version 1.0 and 2.0 protocols
# - handles Json-RPC version 2.0 batch calls
# - independent of transport--can be used directly or within HTTP
# - easy registration of methods for either version 1.0 only, 2.0 only,
#   or both versions
# - can register entire instance (all methods added, but skips any prefixed
#   with '_')
#
##############################################################################

__all__ = [
    "Service"
]

try:
    import simplejson as json_parser
except ImportError, err:
    print "FATAL: json-module 'simplejson' is missing (%s)" % (err)
    import sys
    sys.exit(1)

import traceback

##############################################################################
# Error Codes
##############################################################################

# JSON-RPC 2.0 error-codes
PARSE_ERROR           = -32700
INVALID_REQUEST       = -32600
METHOD_NOT_FOUND      = -32601
INVALID_METHOD_PARAMS = -32602  # invalid number/type of parameters
INTERNAL_ERROR        = -32603  # all other errors

# human-readable messages
ERROR_MESSAGE = {
    PARSE_ERROR           : "Parse error.",
    INVALID_REQUEST       : "Invalid Request.",
    METHOD_NOT_FOUND      : "Method not found.",
    INVALID_METHOD_PARAMS : "Invalid parameters.",
    INTERNAL_ERROR        : "Internal error."
    }


##############################################################################
def make_error(code, data = None):
##############################################################################
    if code not in ERROR_MESSAGE:
        code = INTERNAL_ERROR

    ret = {
        "code":    code,
        "message": ERROR_MESSAGE[code]
        }
    if data is not None:
        ret["data"] = data
    return ret

##############################################################################
def dict_key_clean(d):
##############################################################################
    """Convert all keys of the dict 'd' to (ascii-)strings.

    :Raises: UnicodeEncodeError
    """
    new_d = {}
    for (k, v) in d.iteritems():
        new_d[str(k)] = v
    return new_d


##############################################################################
class Service:
##############################################################################
    """Json-RPC Service Dispatcher base class"""

    ##########################################################################
    def __init__(self,
                 instance  = None,
                 methods   = None, # all rpc version methods
                 methods10 = None, # rpc version 1.0 methods
                 methods20 = None  # rpc version 2.0 methods
                 ):
    ##########################################################################
        self.instances = []

        # Store all attributes of class before any methods are added
        # for negative lookup later
        self.base_attributes = set(dir(self))
        self.base_attributes.add('base_attributes')

        self.methods    = {} # common methods
        self.methods_10 = {} # Json-RPC v1.0-only methods
        self.methods_20 = {} # Json-RPC v2.0-only methods

        #self.methods = {
        #    u'system.list_methods': self.system_list_methods,
        #    u'system.describe':     self.system_describe
        #    }

        if instance is not None:
            self.register_instance(instance)

        if methods is not None:
            for method in methods:
                self.register_method(method)

        if methods10 is not None:
            for method in methods10:
                self.register_10_method(method)

        if methods20 is not None:
            for method in methods20:
                self.register_20_method(method)

    ##########################################################################
    def _from_json(self, json_str):
    ##########################################################################
        return json_parser.loads(json_str)

    ##########################################################################
    def _to_json(self, data):
    ##########################################################################
        return json_parser.dumps(data)

    ##########################################################################
    def register_instance(self, instance, name = None):
    ##########################################################################
        """Register all public methods of a class instance"""
        #for method in instance.__dict__:
        for method in dir(instance):
            if method.startswith('_') is False:
                print "Json-RPC service registering: " + method
                if name is None:
                    self.register_method(getattr(instance, method))
                else:
                    self.register_method(getattr(instance, method),
                                         name = "{0}, {1}".format(name, method))

    ##########################################################################
    def register_method(self, function, name = None):
    ##########################################################################
        """Add a function to the service for any version"""
        self._register_method(function, self.methods, name)

    ##########################################################################
    def register_10_method(self, function, name = None):
    ##########################################################################
        """Add a function to the service for version 1.0 calls"""
        self._register_method(function, self.methods_10, name)

    ##########################################################################
    def register_20_method(self, function, name = None):
    ##########################################################################
        """Add a function to the service for version 2.0 calls"""
        self._register_method(function, self.methods_20, name)

    ##########################################################################
    def _register_method(self, function, methods, name = None):
    ##########################################################################
        """Add function to the service"""
        if name is None:
            try:
                name = function.__name__
            except:
                if hasattr(function, '__call__'):
                    raise "Callable class instances must be passed with name parameter"
                else:
                    raise "Not a function"

        if not name in methods:
            #methods[unicode(name)] = function
            methods[name] = function
        else:
            # TODO: how to handle this?
            # for now allow replacing previous registrations, this allows repl control
            #print "attribute name conflict: {0} already registered".format(name)
            pass

    ##########################################################################
    def get_method(self, method):
    ##########################################################################
        """Search among common methods"""
        return self.methods.get(method, None)

    ##########################################################################
    def get_10_method(self, method):
    ##########################################################################
        """Search among Json-RPC v1.0-only methods, then if not found search
        common methods"""
        ret = self.methods_10.get(method, None)
        if ret is not None:
            return ret
        return self.methods.get(method, None)

    ##########################################################################
    def get_20_method(self, method):
    ##########################################################################
        """Search among Json-RPC v2.0-only methods, then if not found search
        common methods"""
        ret = self.methods_20.get(method, None)
        if ret is not None:
            return ret
        return self.methods.get(method, None)

    ##########################################################################
    def handle_request(self, json_str):
    ##########################################################################
        """
        Process string containing JSON-RPC
        - notifications return None
        - requests return a valid JSON-RPC response
        - batch calls return either None, or an array containing one valid
          JSON-RPC response for each request

        """
        self.last_result = None # clear it

        # TODO: we don't know which version it is if we hit a parse error!
        #       we also don't know the "id"!
        try:
            req = self._from_json(json_str)
        except:
            return {
                "jsonrpc": "2.0",
                "error": make_error(PARSE_ERROR,
                                    data = json_str)
                }

        if isinstance(req, list):
            res = self.json_rpc_20_batch(req)
            if res is None:
                return None;

        elif "jsonrpc" in req:
            if req["jsonrpc"] == "2.0":
                if "id" in req:
                    res = self.json_rpc_20_request(req)
                else:
                    self.json_rpc_20_notification(req)
                    return None
            else:
                res = {
                    "jsonrpc": req["jsonrpc"], # copy the version--we don't understand it
                    "id":      req["id"],
                    "error":   make_error(INVALID_REQUEST,
                                          data = "Bad jsonrpc value")
                    }

        else: # 1.0
            if "id" not in req:
                res = {
                    "result": None,
                    "error": make_error(INVALID_REQUEST,
                                        data = "missing id value")
                    }
            elif req["id"] is None:
                self.json_rpc_10_notification(req)
                return None
            else:
                res = self.json_rpc_10_request(req)

        self.last_result = res

        try:
            res = self._to_json(res)
        except Exception as e:
            res = {
                "jsonrpc": req["jsonrpc"], # copy the version
                "id":      req["id"],
                "error":   make_error(INTERNAL_ERROR,
                                      data = { "exception": str(e),
                                               "traceback": traceback.format_exc()
                                               }
                                      )
                }
            res = self._to_json(res)

        return res

    ##########################################################################
    def json_rpc_20_batch(self, reqs):
    ##########################################################################
        ret = []
        for req in reqs:
            if "id" in req:
                #TODO: these could be done in parallel via internal async
                #      proxied requests
                ret.append(self.json_rpc_20_request(req))
            else:
                self.json_rpc_20_notification(req)

        if 0 < len(ret):
            return ret
        else:
            return None

    ##########################################################################
    def json_rpc_20_request(self, req):
    ##########################################################################        
        fn = self.get_20_method(req.get("method", ""))
        if fn is None:
            return {
                "jsonrpc": "2.0",
                "id":      req["id"],
                "error":   make_error(METHOD_NOT_FOUND,
                                      data = 'method "{0}" not found'.format(req.get("method", "")))
                }
        args = req.get("params", ())

        #convert params-keys from unicode to str
        if isinstance(args, dict):
            try:
                args = dict_key_clean(args)
            except UnicodeEncodeError as e:
                return {
                    "jsonrpc": "2.0",
                    "id":      req["id"],
                    "error":   make_error(INVALID_PARAMS,
                                          data = "Parameter names must be in ASCII.")
                    }
        elif not isinstance(args, (list, tuple)):
            return {
                "jsonrpc": "2.0",
                "id":      req["id"],
                "error":   make_error(INVALID_REQUEST,
                                      date = "Invalid Request, \"params\" must be an array or object.")
                }

        try:
            # see PEP 362 for eventual parameter -> arg matching tests
            if isinstance(args, dict):
                res = fn(**args)
            else:
                res = fn(*args)
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id":      req["id"],
                "error":   make_error(INTERNAL_ERROR,
                                      data = { "exception": str(e),
                                               "traceback": traceback.format_exc()
                                               }
                                      )
                }

        return {
            "jsonrpc": "2.0",
            "id":      req["id"],
            "result":  res
            }

    ##########################################################################
    def json_rpc_20_notification(self, req):
    ##########################################################################
        fn = self.get_20_method(req.get("method", ""))
        if fn is None:
            return None

        args = req.get("params", ())
        try:
            if isinstance(args, dict):
                fn(**args)
            else:
                fn(*args)
        except:
            # notifications are black holes
            pass

        return None

    ##########################################################################
    def json_rpc_10_request(self, req):
    ##########################################################################
        fn = self.get_10_method(req.get("method", ""))
        if fn is None:
            return {
                "id":     req["id"],
                "result": None,
                "error":  make_error(METHOD_NOT_FOUND,
                                     data = 'method "{0}" not found'.format(req.get("method", "")))
                }

        args = req.get("params", ())
        try:
            res = fn(*args)
        except Exception as e:
            return {
                "id":     req["id"],
                "result": None,
                "error":  make_error(INTERNAL_ERROR,
                                      data = { "exception": str(e),
                                               "traceback": traceback.format_exc()
                                               }
                                     )
                }

        return {
            "id":     req["id"],
            "result": res,
            "error":  None
            }

    ##########################################################################
    def json_rpc_10_notification(self, req):
    ##########################################################################
        fn = self.get_10_method(req.get("method", ""))
        if fn is None:
            return None

        args = req.get("params", ())
        try:
            res = fn(*args)
        except:
            # notifications are black holes
            pass

        return None

