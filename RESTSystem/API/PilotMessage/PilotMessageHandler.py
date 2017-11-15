""" Handler redirecting log messages sent by pilots.
    Can be treated as a gateway through which the
    log messages are sent to MQ server or DIRAC service.
"""

import json

import logging
import tornado

from tornado import web, gen
from RESTDIRAC.RESTSystem.Base.RESTHandler import WErr, RESTHandler
from DIRAC import gConfig

class PilotMessageHandler( RESTHandler ):

  ROUTE = "/my"

  @web.asynchronous
  @gen.engine
  def get( self ):
    """ GET method to get configuration data
    :return: requested data
    """
    print "Yeah I got it"
    args = self.request.arguments
    if args.get( 'option' ):
      path = args['option'][0]
      result = yield self.threadTask( gConfig.getOption, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    elif args.get( 'section' ):
      path = args['section'][0]
      result = yield self.threadTask( gConfig.getOptionsDict, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    elif args.get( 'options' ):
      path = args['options'][0]
      result = yield self.threadTask( gConfig.getOptions, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    elif args.get( 'sections' ):
      path = args['sections'][0]
      result = yield self.threadTask( gConfig.getSections, path )
      if not result['OK']:
        raise WErr.fromError( result )
      self.finish( json.dumps( result['Value'] ) )
    else:
      raise WErr( 500, 'Invalid argument' )

  def post( self ):
    """ Post method to change the contents od the Configuration database
    :return: Success or Failure flag
    """
    print 'I got post message\n'
    data = self.get_argument('body', 'No data received')
    data = self.request.body
    print self.request.body 
    self.write('blabla')
    data = tornado.escape.json_decode(self.request.body)
    print data
    self.write('bleble')
    import stomp
    connection = None
    try:
      host_and_port=[('lhcb-test-mb.cern.ch', int(61723))]
      destination="/queue/lhcb.test.*"
      connection = stomp.Connection(host_and_ports=host_and_port, use_ssl = True)
      connection.set_ssl(for_hosts=host_and_port,
                         key_file = '/home/krzemien/workdir/lhcb/dirac_development/etc/grid-security/hostkey.pem',
                         cert_file = '/home/krzemien/workdir/lhcb/dirac_development/etc/grid-security/hostcert.pem',
                         ca_certs = '/home/krzemien/workdir/lhcb/dirac_development/etc/grid-security/allCAs.pem')
      connection.start()
      connection.connect()
    except stomp.exception.ConnectFailedException:
      logging.error( 'Connection error')
    except IOError:
      logging.error('Could not find files with ssl certificates')
    connection.send(destination=destination,
                    body=data)
    #redirect message to MQ
