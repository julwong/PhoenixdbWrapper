# -*- coding: utf-8 -*-

from phoenixdb.avatica import (
    AvaticaClient,pprint,common_pb2,parse_error_page,
    httplib,parse_error_protobuf,errors,logging)
from phoenixdb.connection import Connection
from phoenixdb.cursor import Cursor
from phoenixdb import ProgrammingError
import uuid
import re

__all__ = ["connect", "infer"]

logger = logging.getLogger(__name__)

class LBAvaticaClient(AvaticaClient):
    '''
    add an http header "clientid" for inverse proxy(nginx etc.) load balancing
    '''
    def __init__(self, url, max_retries=None):
        super(LBAvaticaClient, self).__init__(url, max_retries)
        self.clientid = str(uuid.uuid4())

    def _apply(self, request_data, expected_response_type=None):
        logger.debug("Sending request\n%s", pprint.pformat(request_data))

        request_name = request_data.__class__.__name__
        message = common_pb2.WireMessage()
        message.name = 'org.apache.calcite.avatica.proto.Requests${}'.format(request_name)
        message.wrapped_message = request_data.SerializeToString()
        body = message.SerializeToString()
        headers = {
            'content-type': 'application/x-google-protobuf',
            'clientid':self.clientid,
        }

        response = self._post_request(body, headers)
        response_body = response.read()

        if response.status != httplib.OK:
            logger.debug("Received response\n%s", response_body)
            if b'<html>' in response_body:
                parse_error_page(response_body)
            else:
                # assume the response is in protobuf format
                parse_error_protobuf(response_body)
            raise errors.InterfaceError('RPC request returned invalid status code', response.status)

        message = common_pb2.WireMessage()
        message.ParseFromString(response_body)

        logger.debug("Received response\n%s", message)

        if expected_response_type is None:
            expected_response_type = request_name.replace('Request', 'Response')

        expected_response_type = 'org.apache.calcite.avatica.proto.Responses$' + expected_response_type
        if message.name != expected_response_type:
            raise errors.InterfaceError('unexpected response type "{}"'.format(message.name))

        return message.wrapped_message

class AutoAddColumnCursor(Cursor):
    '''
    auto add column schema when Undefined column exception occured
    '''
    def execute(self, operation, parameters=None, hint=None):
        '''
        :param autocommit:
            column definition in such form:
                {
                    "table":"MY_TABLE",
                    "columns":{
                        "C1":"BIGINT",
                        "C2":"VARCHAR"
                    }
                }
        '''
        is_to_retry = True
        while is_to_retry:
            is_to_retry = False # avoid 
            try:
                super(AutoAddColumnCursor, self).execute(operation, parameters)
            except ProgrammingError as e:
                if hint is None:
                    raise e
                m = re.search('Undefined column. columnName=(.+)',e.message)
                if m is None:
                    raise e
                table = hint.get('table')
                if not isinstance(table, str) or len(table) == 0:
                    raise e
                columns = hint.get('columns')
                if not isinstance(columns, dict):
                    raise e
                col_name = m.group(1)
                column = columns.get(col_name)
                if not isinstance(column, str) or len(column) == 0:
                    raise e
                op = "ALTER TABLE %s ADD %s %s" % (table, col_name, column)
                super(AutoAddColumnCursor, self).execute(op)
                del columns[col_name] # we tried
                is_to_retry = True # and we retry

# same as phoenixdb.connect, except that we use 
def connect(url, max_retries=None, cursor_factory=AutoAddColumnCursor, **kwargs):
    """Connects to a Phoenix query server.

    :param url:
        URL to the Phoenix query server, e.g. ``http://localhost:8765/``

    :param autocommit:
        Switch the connection to autocommit mode.

    :param readonly:
        Switch the connection to readonly mode.

    :param max_retries:
        The maximum number of retries in case there is a connection error.

    :param cursor_factory:
        If specified, the connection's :attr:`~phoenixdb.connection.Connection.cursor_factory` is set to it.

    :returns:
        :class:`~phoenixdb.connection.Connection` object.
    """
    client = LBAvaticaClient(url, max_retries=max_retries)
    client.connect()
    return Connection(client, cursor_factory=cursor_factory, **kwargs)

def infer(v):
    '''
    infer column definition of a value, redefine the value(if necessary)

    :param url:
        value to infer
    '''
    column_define, value_define = None, None
    if isinstance(v, str):
        column_define = 'VARCHAR'
        value_define = v
    elif isinstance(v, int):
        column_define = 'BIGINT'
        value_define = v
    elif isinstance(v, float):
        column_define = 'DOUBLE'
        value_define = v
    elif isinstance(v, list):
        data_type = None
        for i in v:
            t = infer(i)
            if t not in ('VARCHAR', 'BIGINT', 'DOUBLE'):
                raise Exception('unsupported array item type: %s' % t)
            if data_type is None:
                data_type = t
            elif data_type != t:
                raise Exception('consensus item type must be satisfied')
        column_define = 'ARRAY[%s]' % data_type
        value_define = v
    else:
        raise Exception("unsupported data type: %s" % type(v))

    return column_define, value_define
