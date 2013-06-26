from django.shortcuts import render_to_response
from monitor.views import respond
import os
import string

from models import ZNode

def istext(s, text_chars="".join(map(chr, range(32, 127))) + "\n\r\t\b"):
    if "\0" in s: return False
    if not s: return True
    t = s.translate(string.maketrans("", ""), text_chars)
    return len(t) == 0

def index(request, addrs, path=""):
    path = "/" + path
    try:
        parent_path = os.path.dirname(path)
        znode = ZNode(addrs, path)
        znode.children.sort()
        if not istext(znode.data):
            znode.data = "0x" + "".join(["%d" % (ord(d)) for d in znode.data])
            znode.datatype = "bin"
        else:
            znode.datatype = "str"

        params = {'znode':znode,
                  'addrs':addrs,
                  'parent_path':parent_path}
        return respond(request, 'zktree/index.html', params)
    except Exception as err:
        return respond(request, 'zktree/error.html',
                       {'error':str(err)})
