#!/usr/bin/env python
#

import httplib
import mimetypes
import os
import urllib

class TankClient:
  '''
  The package server client.
  '''
  def __init__(self, host, port=80, upload_uri='/upload_package/',
      check_uri='/check_package/'):
    self.conn = httplib.HTTPConnection(host, port)
    self.upload_uri = upload_uri
    self.check_uri = check_uri

  def check_package(self, artifact, checksum):
    '''
    Check whether a package of specified artifact and checksum already
    existed on the package server.

    @param  artifact the package artifact
    @param  checksum the package checksum
    @return string   the package infomation if the package already existed,
                     otherwise None
    '''
    data = urllib.urlencode({
        'artifact': artifact,
        'checksum': checksum,
    })

    self.conn.request('GET', '%s?%s' % (self.check_uri, data))
    response = self.conn.getresponse()

    if response.status == 200:
      body = response.read()
      if body.startswith('{'):
        return body
    return None

  def upload(self, package_path, artifact, revision):
    '''
    Upload the specified package to the package server.

    @param  package_path the package path
    @param  artifact     the package artifact
    @param  revision     the package revision
    @return integer      the http status code
    '''
    param = {
      'artifact': artifact,
      'revision': revision,
    }

    content_type, body = self._encode_multipart_formdata(param,
        [('file', package_path, open(package_path, 'rb').read())])

    headers = {
      'Content-Type': content_type,
      'Content-Length': len(body),
    }

    self.conn.request('POST', self.upload_uri, body, headers)
    response = self.conn.getresponse()
    return response.status

  def _encode_multipart_formdata(self, fields, files):
    LIMIT = '----------lImIt_of_THE_fIle_eW_$'
    CRLF = '\r\n'
    L = []

    for (key, value) in fields.iteritems():
      L.append('--' + LIMIT)
      L.append('Content-Disposition: form-data; name="%s"' % key)
      L.append('')
      L.append(value)

    for (key, filename, value) in files:
      L.append('--' + LIMIT)
      L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
      L.append('Content-Type: %s' % self._get_content_type(filename))
      L.append('')
      L.append(value)
      L.append('--' + LIMIT + '--')
      L.append('')

    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % LIMIT
    return content_type, body

  def _get_content_type(self, filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'

if __name__ == '__main__':
  test()
