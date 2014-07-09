from django.contrib import admin

from models import Machine


class MachineAdmin(admin.ModelAdmin):
  list_display = ('hostname', 'ip', 'idc', 'rack', 'cores', 'ram',
      'disks', 'disk_capacity', 'ssds', 'ssd_capacity', )
  list_filter = ('idc', 'rack', )
  ordering = ('hostname', )


admin.site.register(Machine, MachineAdmin)
