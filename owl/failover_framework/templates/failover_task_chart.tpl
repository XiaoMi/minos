<script type="text/javascript">

$(function () {
  var chart;
  var counter = 0;
  var colors = [];
  {% for task in tasks %}
    {% if task.success %}
      colors[counter] = "#2c86ff";
    {% else %}
      colors[counter] = "#ff2c86";
    {% endif %}
    counter++;
  {% endfor %}
  counter = 0;

  $(document).ready(function() {
    chart = new Highcharts.Chart({
      chart: {
        renderTo: '{{ chart_id }}',
        type: 'column',
        zoomType: 'x',
      },
      title: {
        text: '{{ chart_title }}'
      },
      credits: {
        enable: false,
      },
      xAxis: {
        categories: [
          {% for task in tasks %}
            '{{ task.start_time }}',
          {% endfor %}
        ],
        labels: {
          rotation: -45,
          align: 'right',
        }
      },
      yAxis: {
        min: 0,
        title: {
          text: 'Action Number'
        }
      },
      plotOptions: {
        series: {
          cursor: 'pointer',
          point: {
            events: {
              click: function() {
                window.location = "/failover/task/?start_time="+this.category;
              }
            }
          }
        }
      },
      series: [{
        name: 'Action Number',
        data: [
          {% for task in tasks %}
            {y: {{ task.action_number }}, color: colors[counter++]},
          {% endfor %}
        ],
      }]
    });
  });
});

</script>
<div id="{{ chart_id }}"></div>