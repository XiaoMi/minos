from django import forms
from django.db import models
from tank.settings import MEDIA_URL

# Create your models here.

def get_upload_path(instance, filename):
  return '%s/%s-%s/%s' % (instance.artifact, instance.revision,
      instance.timestamp, instance.name)

class Package(models.Model):
  artifact = models.CharField(max_length=128)
  name = models.CharField(max_length=256)
  revision = models.CharField(max_length=64)
  timestamp = models.CharField(max_length=128)
  checksum = models.CharField(max_length=128)
  file = models.FileField(upload_to=get_upload_path)

  def __unicode__(self):
    return u'%s %s %s %s %s' % (self.artifact, self.revision,
        self.timestamp, self.name, self.checksum)

  def __str__(self):
    field_json_str = "{" \
      "'artifact': '%s'," \
      "'package_name': '%s'," \
      "'revision': '%s'," \
      "'timestamp': '%s'," \
      "'checksum': '%s'" \
    "}" % (
        self.artifact, self.name,
        self.revision, self.timestamp,
        self.checksum)
    return field_json_str

  def download_link(self):
    return '%s/%s/%s-%s/%s' % (MEDIA_URL.rstrip('/'), self.artifact,
        self.revision, self.timestamp, self.name)
