# extended filter for django template

from django import template
import utils.quota_util

register = template.Library()

# generate param for group
@register.filter(name='param_group')
def param_group(graph_config) :
  return '|'.join([group for group, key in graph_config])

# generate param for key in graph
@register.filter(name='param_key')
def param_key(graph_config):
  return '|'.join(['-'.join((group,key)) for group, key, unit in graph_config])

# generate param for multikey in view
@register.filter(name='param_multikey_for_view')
def param_multikey_for_view(view_config):
  return '|'.join([param_key(graph_config) for graph_config in view_config])

# generate param for multikey in view
@register.filter(name='param_height')
def param_height(view_config):
  graph_per_row = 3
  height_per_row = 295
  return (len(view_config) + (graph_per_row - 1)) / graph_per_row * height_per_row

# generate picture width
@register.filter(name='pic_width')
def pic_width(span):
  return span * 100

# generate picture height
@register.filter(name='pic_heigth')
def pic_heigth(metrics):
  return len(metrics) * 10 + 450

# format big number
@register.filter(name='format_bigint')
def format_bigint(value):
  try:
    value = int(value)
  except (TypeError, ValueError):
    return value

  if value < 1024*1024:
    return value

  K = 1024
  formaters = (
    (2, '%.2fM'),
    (3, '%.2fG'),
    (4, '%.2fT'),
    (5, '%.2fP'),
  )

  for exponent, formater in formaters:
    larger_num = K ** exponent
    if value < larger_num * K:
      return formater % (value/float(larger_num))

# is space quota healthy
@register.filter(name='is_space_quota_healthy')
def is_space_quota_healthy(total, used):
  return utils.quota_util.is_space_quota_healthy(total, used)

# is name quota healthy
@register.filter(name='is_name_quota_healthy')
def is_name_quota_healthy(total, used):
  return utils.quota_util.is_name_quota_healthy(total, used)
