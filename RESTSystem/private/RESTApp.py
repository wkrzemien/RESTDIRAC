import ssl
from tornado import web, httpserver, ioloop, process, autoreload
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from RESTDIRAC.RESTSystem.Base.RESTHandler import RESTHandler
from RESTDIRAC.ConfigurationSystem.Client.Helpers import RESTConf

class RESTApp( object ):

  def __init__( self ):
    self.__handlers = {}
    self.__routes = []
    self.__app = None
    self.__sslops = None
    self.__httpSrv = None

  def _logRequest( self, handler ):
    status = handler.get_status()
    if status < 400:
      logm = gLogger.notice
    elif status < 500:
      logm = gLogger.warn
    else:
      logm = gLogger.error
    request_time = 1000.0 * handler.request.request_time()
    logm( "%d %s %.2fms" % ( status, handler._request_summary(), request_time ) )

  def __reloadAppCB( self ):
    gLogger.notice( "\n !!!!!! Reloading web app...\n" )

  def bootstrap( self, reFilter = None ):
    gLogger.always( "\n  === Bootstrapping REST Server ===  \n" )
    ol = ObjectLoader( [ 'DIRAC', 'RESTDIRAC' ] )
    result = ol.getObjects("RESTSystem.API", parentClass = RESTHandler, recurse = True, reFilter = reFilter)
    if not result[ 'OK' ]:
      return result

    self.__handlers = result[ 'Value' ]
    if not self.__handlers:
      return S_ERROR( "No handlers found" )

    self.__routes = [(self.__handlers[k].getRoute(),
                      self.__handlers[k]) for k in self.__handlers if self.__handlers[ k ].getRoute()]
    gLogger.info( "Routes found:" )
    for t in sorted( self.__routes ):
      gLogger.info( " - %s : %s" % ( t[0], t[1].__name__ ) )

    balancer = RESTConf.balancer()
    kw = dict( debug = RESTConf.debug(), log_function = self._logRequest )
    if balancer and RESTConf.numProcesses not in ( 0, 1 ):
      process.fork_processes( RESTConf.numProcesses(), max_restarts = 0 )
      kw[ 'debug' ] = False
    if kw[ 'debug' ]:
      gLogger.always( "Starting in debug mode" )
    self.__app = web.Application( self.__routes, **kw )
    port = RESTConf.port()
    if balancer:
      gLogger.notice( "Configuring REST HTTP service for balancer %s on port %s" % ( balancer, port ) )
      self.__sslops = False
    else:
      gLogger.notice( "Configuring REST HTTPS service on port %s" % port )
      self.__sslops = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
      self.__sslops.load_cert_chain(RESTConf.cert(), RESTConf.key())
    self.__httpSrv = httpserver.HTTPServer( self.__app, ssl_options = self.__sslops )
    self.__httpSrv.listen( port )
    return S_OK()

  def run( self ):
    port = RESTConf.port()
    if self.__sslops:
      url = "https://0.0.0.0:%s" % port
    else:
      url = "http://0.0.0.0:%s" % port
    gLogger.always( "Starting REST server on %s" % url )
    autoreload.add_reload_hook( self.__reloadAppCB )
    ioloop.IOLoop.instance().start()
