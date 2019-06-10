import socket

import six


def get_local_addr():
    """ Get the local IP address (that isn't localhost) """
    _, _, ipaddrlist = socket.gethostbyname_ex(socket.gethostname())
    try:
        return next(ip for ip in ipaddrlist if ip not in ('127.0.0.1',))
    except StopIteration:
        raise ValueError('Cannot find non-loopback ip address for this server (received %s)' % ', '.join(ipaddrlist))


def resolve_addr(addr):
    """ Resolve address (IP string of host name) into an IP string. """
    try:
        socket.inet_aton(addr)
        return addr
    except socket.error:
        try:
            return socket.gethostbyname(addr)
        except socket.error:
            pass


def parse_return_stack_on_code(codes):
    assert isinstance(codes, list), "return_stack_on_code must be a list"

    def parse(e):
        if isinstance(e, six.integer_types):
            code, subcodes = e, None
        elif isinstance(e, (list, tuple)):
            code, subcodes = e[:2]
            assert isinstance(code, six.integer_types), "return_stack_on_code/code must be int"
            if isinstance(subcodes, six.integer_types):
                subcodes = [subcodes]
            if isinstance(subcodes, (list, tuple)):
                assert all(isinstance(x, six.integer_types) for x in subcodes),\
                    "return_stack_on_code/subcode must be list(int)"
            else:
                raise ValueError("invalid return_stack_on_code/subcode(s): %s" % subcodes)
        else:
            raise ValueError("invalid return_stack_on_code/subcode(s): %s" % e)
        return code, subcodes

    return dict(map(parse, codes))
