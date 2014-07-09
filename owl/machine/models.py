from django.contrib.auth import models as auth_models
from django.db import models
from django.db.models.signals import pre_save, post_save
from django.dispatch import receiver


@receiver(pre_save, sender=auth_models.User)
def auth_user_pre_save(sender, instance=None, **kwargs):
  instance.is_staff = True
