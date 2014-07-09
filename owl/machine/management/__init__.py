from django.conf import settings
from django.contrib.auth.models import User
from django.db.models.signals import post_syncdb
from django.dispatch import receiver


@receiver(post_syncdb, dispatch_uid='machine.load_builtin_data')
def load_builtin_data(sender, **kwargs):
  # add several pre-defined admin users or change them as superusers.
  for name, email in settings.ADMINS:
    try:
      user = User.objects.get(username=name)
      user.is_superuser = True
      user.email = email
    except User.DoesNotExist:
      user = User(username=name, is_superuser=True, email=email)

    user.save()

  # set all others as non-superusers.
  User.objects.exclude(username__in=[name for name, email in settings.ADMINS]
      ).update(is_superuser=False)
