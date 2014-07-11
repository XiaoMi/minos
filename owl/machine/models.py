from django.contrib.auth import models as auth_models
from django.db import models
from django.db.models.signals import pre_save, post_save
from django.dispatch import receiver


class LowerCaseCharField(models.CharField):
  """
  Defines a charfield which automatically converts all inputs to
  lowercase and saves.
  """

  def pre_save(self, model_instance, add):
    """
    Converts the string to lowercase before saving.
    """
    current_value = getattr(model_instance, self.attname)
    setattr(model_instance, self.attname, current_value.lower())
    return getattr(model_instance, self.attname)


class Machine(models.Model):
  # Identifier
  hostname = LowerCaseCharField(max_length=64, unique=True)
  ip = models.IPAddressField(unique=True)

  # Location
  idc = LowerCaseCharField(max_length=8)
  rack = LowerCaseCharField(max_length=8)

  # Capacity
  cores = models.IntegerField()
  ram = models.IntegerField(
      help_text='RAM in G bytes')
  disks = models.IntegerField(
      help_text='Number of disks')
  disk_capacity = models.IntegerField(
      help_text='Capaciy of each disk in G bytes')
  ssds = models.IntegerField(default=0,
      help_text='Number of SSDs')
  ssd_capacity = models.IntegerField(default=0,
      help_text='Capaciy of each SSD in G bytes')

  # Trace record change
  create_time = models.DateTimeField(auto_now_add=True)
  update_time = models.DateTimeField(auto_now=True)


@receiver(pre_save, sender=auth_models.User)
def auth_user_pre_save(sender, instance=None, **kwargs):
  instance.is_staff = True
