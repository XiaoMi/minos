from django.contrib.auth.models import User
from django.db.models.signals import post_syncdb
from django.dispatch import receiver

from owl_config import DJANGO_ADMINS


@receiver(post_syncdb, dispatch_uid='machine.load_builtin_data')
def load_builtin_data(sender, **kwargs):
  # add several pre-defined admin users or change them as superusers.
  for name in DJANGO_ADMINS:
    # name is possibly an email address
    pos = name.find('@')
    if pos >= 0:
      email = name
      name = name[:pos]
    else:
      email = None

    try:
      user = User.objects.get(username=name)
      user.is_superuser = True
      user.email = email
    except User.DoesNotExist:
      user = User(username=name, is_superuser=True, email=email)

    user.save()
