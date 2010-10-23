import urlparse
import time
import urllib
import hmac
import base64
import datetime
import httplib
import socket
from time import sleep
from collections import defaultdict
from orderedset import OrderedSet

import settings
import status

try:
    import xml.etree.ElementTree as ET
except ImportError:
    import elementtree.ElementTree as ET

class SimpleDBError(Exception):
    """We use this exception to represent errors returned by simpledb"""
    pass

class SimpleDBFailure(Exception):
    """We use this exception for sdb connection failures"""
    def __init__(self, error):
        self.error = error

def urlencode(d):
    if isinstance(d, dict):
        d = d.iteritems()
    return '&'.join(['%s=%s' % (escape(k), escape(v)) for k, v in d])

def _utf8_str(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return str(s)

def escape(s):
    return urllib.quote(s, safe='-_~')

def generate_timestamp():
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())

class SignatureMethod(object):

    @property
    def name(self):
        raise NotImplementedError

    def build_signature_base_string(self, request):
        sig = '\n'.join((
            request.get_normalized_http_method(),
            request.get_normalized_http_host(),
            request.get_normalized_http_path(),
            request.get_normalized_parameters(),
        ))
        return sig

    def build_signature(self, request, aws_secret):
        raise NotImplementedError

class SignatureMethod_HMAC_SHA1(SignatureMethod):
    name = 'HmacSHA1'
    version = '2'

    def build_signature(self, request, aws_secret):
        base = self.build_signature_base_string(request)
        try:
            import hashlib # 2.5
            hashed = hmac.new(aws_secret, base, hashlib.sha1)
        except ImportError:
            import sha # deprecated
            hashed = hmac.new(aws_secret, base, sha)
        return base64.b64encode(hashed.digest())

class SignatureMethod_HMAC_SHA256(SignatureMethod):
    name = 'HmacSHA256'
    version = '2'

    def build_signature(self, request, aws_secret):
        import hashlib
        base = self.build_signature_base_string(request)
        hashed = hmac.new(aws_secret, base, hashlib.sha256)
        return base64.b64encode(hashed.digest())

class Response(object):
    def __init__(self, response, content, request_id, usage):
        self.response = response
        self.content = content
        self.request_id = request_id
        self.usage = usage

class Request(object):
    def __init__(self, method, url, parameters=None):
        self.method = method
        self.url = url
        self.parameters = parameters or {}

    def set_parameter(self, name, value):
        self.parameters[name] = value

    def get_parameter(self, parameter):
        try:
            return self.parameters[parameter]
        except KeyError:
            raise SimpleDBError('Parameter not found: %s' % parameter)

    def to_postdata(self):
        return urlencode([(_utf8_str(k), _utf8_str(v)) for k, v in self.parameters.iteritems()])

    def get_normalized_parameters(self):
        """
        Returns a list constisting of all the parameters required in the
        signature in the proper order.

        """
        return urlencode([(_utf8_str(k), _utf8_str(v)) for k, v in 
                            sorted(self.parameters.iteritems()) 
                            if k != 'Signature'])

    def get_normalized_http_method(self):
        return self.method.upper()

    def get_normalized_http_path(self):
        parts = urlparse.urlparse(self.url)
        if not parts[2]:
            # For an empty path use '/'
            return '/'
        return parts[2]

    def get_normalized_http_host(self):
        parts = urlparse.urlparse(self.url)
        return parts[1].lower()

    def sign_request(self, signature_method, aws_key, aws_secret):
        self.set_parameter('AWSAccessKeyId', aws_key)
        self.set_parameter('SignatureVersion', signature_method.version)
        self.set_parameter('SignatureMethod', signature_method.name)
        self.set_parameter('Timestamp', generate_timestamp())
        self.set_parameter('Signature', signature_method.build_signature(self, aws_secret))

class SimpleDB(object):
    """Represents a connection to Amazon SimpleDB."""

    ns = 'http://sdb.amazonaws.com/doc/2009-04-15/'
    service_version = '2009-04-15'
    try:
        import hashlib # 2.5+
        signature_method = SignatureMethod_HMAC_SHA256
    except ImportError:
        signature_method = SignatureMethod_HMAC_SHA1

    def __init__(self,
                 aws_access_key=None,
                 aws_secret_access_key=None,
                 db=None, 
                 secure=True
                ):
        """
        Use your `aws_access_key` and `aws_secret_access_key` to create a connection to
        Amazon SimpleDB.

        SimpleDB requests are directed to the host specified by `db`, which defaults to
        ``sdb.amazonaws.com``.

        The optional `secure` argument specifies whether HTTPS should be used. The 
        default value is ``True``.
        """

        if aws_access_key is not None:
            self.aws_key = aws_access_key
        else:
            self.aws_key = settings.amazon_access_key_id

        if aws_secret_access_key is not None:
            self.aws_secret = aws_secret_access_key
        else:
            self.aws_secret = settings.amazon_secret_access_key

        if db is not None:
            self.db = db
        else:
            self.db = settings.amazon_db

        if secure:
            self.scheme = 'https'
        else:
            self.scheme = 'http'

        self.last_http_object_initialization = None

        self.sdb_connection = \
            httplib.HTTPSConnection(self.db, timeout=settings.amazon_timeout)

        # The following list lists simpledb request parameters that doesn't
        # relate to the request's action. We will ignore this parameters when
        # generating the attribute 'action_parameters' for the request's item in
        # status.requests
        self.request_connection_parameters = [
                                              'SignatureVersion',
                                              'AWSAccessKeyId',
                                              'Timestamp',
                                              'SignatureMethod',
                                              'Version',
                                              'Signature',
                                              'Action'
                                             ]
    def __make_request(self, request):
        headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8', 
                   'host': self.db}
        request.set_parameter('Version', self.service_version)
        request.sign_request(self.signature_method(), self.aws_key, self.aws_secret)

        time_request_begin = datetime.datetime.utcnow()

        self.sdb_connection.request(
                                    request.method,
                                    '/',
                                    request.to_postdata(),
                                    headers
                                   )


        execution_time = datetime.datetime.utcnow() - time_request_begin

        response = self.sdb_connection.getresponse()

        response_headers = dict(response.getheaders())
        response_headers['status'] = response.status

        response_content = response.read()

        e = ET.fromstring(response_content)

        error = e.find('Errors/Error')
        if error:
            raise SimpleDBError(error.find('Message').text)

        meta = e.find('{%s}ResponseMetadata' % self.ns)
        request_id = meta.find('{%s}RequestId' % self.ns).text
        usage = meta.find('{%s}BoxUsage' % self.ns).text

        action_parameters = \
            dict([
                  (parameter, value) for \
                  parameter, value in \
                  request.parameters.items() if \
                  parameter not in self.request_connection_parameters
                 ])

        status.requests.append(
                               {
                                'execution_time': execution_time,
                                'db_box_usage': usage,
                                'action': request.parameters['Action'],
                                'action_parameters': action_parameters,
                                'request': request,
                                'request_id': request_id,
                                'response_headers': response_headers,
                                'response_content': response_content
                               }
                              )

        status.last_data_resource_used = 'key_value_db'

        return Response(response_headers, response_content, request_id, usage)

    def _make_request(self, request):
        delay_before_attempts = [0] # lists the delay before each attempt
        delay_before_attempts.extend(settings.amazon_timeout_retries_delay)

        for delay in delay_before_attempts:
            try:
                sleep(delay)
                return self.__make_request(request)
            except socket.timeout as error: 
                status.https_timeouts += 1 # keep in status timeout log

        # If all our attempts failed
        raise SimpleDBFailure('Timeout')

    def _sdb_url(self):
        return urlparse.urlunparse((self.scheme, self.db, '', '', '', ''))

    def create_domain(self, name):
        """
        Creates a new domain.

        The domain `name` argument must be a string, and must be unique among 
        the domains associated with your AWS Access Key. The CreateDomain operation 
        may take 10 or more seconds to complete. By default, you can create up to 
        100 domains per account.

        Returns the newly created `Domain` object.
        """

        data = {
            'Action': 'CreateDomain',
            'DomainName': name,
        }
        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)
        return name
    
    def delete_domain(self, domain):
        """
        Deletes a domain. Any items (and their attributes) in the domain are
        deleted as well. The DeleteDomain operation may take 10 or more seconds
        to complete.

        The `domain` argument can be a string representing the name of the 
        domain, or a `Domain` object.
        """

        data = {
            'Action': 'DeleteDomain',
            'DomainName': domain,
        }
        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def _list_domains(self):
        # Generator that yields each domain associated with the AWS Access Key.
        data = {
            'Action': 'ListDomains',
            'MaxNumberOfDomains': '100',
        }

        while True:
            request = Request("POST", self._sdb_url(), data)
            response = self._make_request(request)

            e = ET.fromstring(response.content)
            domain_result = e.find('{%s}ListDomainsResult' % self.ns)
            if domain_result:
                domain_names = domain_result.findall('{%s}DomainName' % self.ns)
                for domain in domain_names:
                    yield domain.text

                # SimpleDB will return a max of 100 domains per request, and
                # will return a NextToken if there are more.
                next_token = domain_result.find('{%s}NextToken' % self.ns)
                if next_token is None:
                    break
                data['NextToken'] = next_token.text
            else:
                break

    def list_domains(self):
        """
        Lists all domains associated with your AWS Access Key.
        """
        return list(self._list_domains())

    def has_domain(self, domain):
        return domain in [d for d in self.list_domains()]

    def get_domain_metadata(self, domain):
        """Returns information about the domain. Includes when the domain was
        created, the number of items and attributes, and the size of attribute
        names and values.

        The `domain` argument can be a string representing the name of the
        domain or a `Domain` object.
        """

        if isinstance(domain, Domain):
            domain = domain.name
        data = {
            'Action': 'DomainMetadata',
            'DomainName': domain,
        }
        request = Request("POST", self._sdb_url(), data)
        response = self._make_request(request)

        e = ET.fromstring(response.content)
        metadata = {}
        metadata_result = e.find('{%s}DomainMetadataResult' % self.ns)
        if metadata_result is not None:
            for child in metadata_result.getchildren():
                tag, text = child.tag, child.text
                if tag.startswith('{%s}' % self.ns):
                    tag = tag[42:] # Die ElementTree namespaces, die!
                metadata[tag] = text
        return metadata

    def put_attributes(self, domain, item, attributes):
        """Creates or replaces attributes in an item.

        Attributes should be dictionary in which the keys are names of
        attributes and the values are dictionaries with the keys 'values' and
        'replace'

        values should be set of strings.
        Previously for single value it was o.k. to pass the value outside of a
        set but this behavior changed in order To make get_attributes() output
        more intuitive and to reflect better the fact that each simpledb item
        constructed like some kind of a dictionary of sets.

        replace:
        if replace is True the new values will replace previous values their
        attribute had, otherwise the new values will be appended to the ones
        already exists in simpledb.

        Valid 'attributes' parameter example:
        {
            'a': {
                  'values': ['a', 'b'],
                  'replace': False
                 },
            'b': {
                  'values': ['c', 'd'],
                  'replace': True
                 },
        }

        Remember: in simpledb attribute's values is a set (there can't be
        duplicate values).

        """

        data = {
            'Action': 'PutAttributes',
            'DomainName': domain,
            'ItemName': item,
        }

        idx = 0
        for attribute_name, attribute_params in attributes.items():
            for value in attribute_params['values']:
                data['Attribute.%s.Name' % idx] = attribute_name
                data['Attribute.%s.Value' % idx] = value

                if attribute_params['replace']:
                    data['Attribute.%s.Replace' % idx] = 'true'

                idx += 1

        status.actions_count['put_attributes'] += 1

        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def batch_put_attributes(self, domain, items):
        """
        Performs multiple PutAttribute operations in a single call. This yields
        savings in round trips and latencies and enables SimpleDB to optimize
        your request, which generally yields better throughput.

        items should be dictionary in which the keys are items names and
        the values are dictionaries with the structure of put_attributes()'s
        'attributes' parameter.

        """

        data = {
            'Action': 'BatchPutAttributes',
            'DomainName': domain,
        }

        for item_id, (item_name, changes_dictionary) in enumerate(items.items()):
            data['Item.%s.ItemName' % item_id] = item_name

            attr_id = 0
            for attribute_name, attribute_params in changes_dictionary.items():
                for value in attribute_params['values']:
                    data['Item.%s.Attribute.%s.Name' % (item_id, attr_id)] = attribute_name
                    data['Item.%s.Attribute.%s.Value' % (item_id, attr_id)] = value

                    if attribute_params['replace']:
                        data['Item.%s.Attribute.%s.Replace' % (item_id, attr_id)] = 'true'

                    attr_id += 1

        status.actions_count['put_attributes'] += 1

        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def delete_attributes(self, domain, item, attributes=None):
        """Deletes one or more attributes associated with an item. If all attributes of
        an item are deleted, the item is deleted.

        If the optional parameter `attributes` is not provided, all attributes
        are deleted.

        """
        
        if attributes is None:
            attributes = {}

        data = {
            'Action': 'DeleteAttributes',
            'DomainName': domain,
            'ItemName': item,
        }

        idx = 0
        for name, values in attributes.iteritems():
            if values: # if values isn't empty or None
                if not hasattr(values, '__iter__') or isinstance(values, basestring):
                    values = [values]
                for value in values:
                    data['Attribute.%s.Name' % idx] = name
                    data['Attribute.%s.Value' % idx] = value
                    idx += 1
            else:
                data['Attribute.%s.Name' % idx] = name
                idx += 1
        
        if not attributes: # if attributes is empty, this is item delete request
            status.actions_count['delete_item'] += 1
        else:
            status.actions_count['delete_attributes'] += 1

        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def get_attributes(self, domain, item, attributes=None):
        """
        Returns all of the attributes associated with the item.
        
        The returned attributes can be limited by passing a list of attribute
        names in the optional `attributes` argument.

        If the item does not exist, an empty set is returned. An error is not
        raised because SimpleDB provides no guarantee that the item does not
        exist on another replica. In other words, if you fetch attributes that 
        should exist, but get an empty set, you may have better luck if you try
        again in a few hundred milliseconds.
        """

        data = {
            'Action': 'GetAttributes',
            'DomainName': domain,
            'ItemName': item,
        }

        if attributes:
            for i, attr in enumerate(attributes):
                data['AttributeName.%s' % i] = attr
        request = Request("POST", self._sdb_url(), data)
        response = self._make_request(request)
        
        # update status
        status.actions_count['get_item'] += 1

        # Initiate the result array
        result = defaultdict(set)
        # If we specify specific attributes in the get_attribute query we
        # initiate them. This because we want them to hold empty set if they
        # have no values in simpledb, otherwise the output will be less
        # intuitive.
        for attribute in (attributes or []):
            result[attribute] = OrderedSet()

        e = ET.fromstring(response.content)
        attr_node = e.find('{%s}GetAttributesResult' % self.ns)

        if attr_node:
            result.update(self._parse_attributes(domain, attr_node))

        return result

    def _parse_attributes(self, domain, attribute_node):
        # attribute_node should be an ElementTree node containing Attribute
        # child elements.

        # previously, this method returned attributes values as a list only if
        # it had more than one value. This behavior changed to make output more
        # easy to work with, and to reflect better the fact that each simpledb
        # item constructed like some kind of a dictionary of sets.

        # We use OrderedSet to keep the order in which the values returned from
        # simpledb
        attributes = defaultdict(OrderedSet)

        for attribute in attribute_node.findall('{%s}Attribute' % self.ns):
            name = attribute.find('{%s}Name' % self.ns).text
            value = attribute.find('{%s}Value' % self.ns).text

            attributes[name].add(value)

        return attributes

    def _select(self, domain, query):
        data = {
            'Action': 'Select',
            'SelectExpression': query,
        }

        while True:
            status.actions_count['select'] += 1
            request = Request("POST", self._sdb_url(), data)
            response = self._make_request(request)

            e = ET.fromstring(response.content)
            item_node = e.find('{%s}SelectResult' % self.ns)
            if item_node is not None:
                for item in item_node.findall('{%s}Item' % self.ns):
                    name = item.findtext('{%s}Name' % self.ns)
                    attributes = self._parse_attributes(domain, item)
                    yield {name: attributes}

                # SimpleDB will return a max of 100 items per request, and
                # will return a NextToken if there are more.
                next_token = item_node.find('{%s}NextToken' % self.ns)
                if next_token is None:
                    break
                data['NextToken'] = next_token.text
            else:
                break

    def select(self, output_list, domain_name, expression=None, sort_instructions=None, limit=None):
        # If output_list holds attribute name but isn't compound type, we
        # enclose it in a list
        if not hasattr(output_list, '__iter__') or isinstance(output_list, basestring):
            if not output_list in ['*', 'itemName()', 'count(*)']:
                output_list = [output_list]
        # If output_list is list it means it holds explicit list of attributes
        if hasattr(output_list, '__iter__'):
            output_list = ','.join(["`%s`" % i.replace('`', '``') for i in output_list])

        query = "select %(output_list)s from `%(domain_name)s`" % \
            {'output_list': output_list, 'domain_name': domain_name}
        if expression is not None:
            query += ' where ' + expression
            # The sort attribute must be present in at least one of the
            # predicates of the expression. 
            if sort_instructions is not None:
                query += ' order by ' + sort_instructions
        if limit is not None:
            query += ' limit ' + str(limit)

        return list(self._select(domain_name, query))

    def __iter__(self):
        return self._list_domains()
