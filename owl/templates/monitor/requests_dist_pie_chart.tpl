<script type="text/javascript">
    var chart;
    jQuery(document).ready(function() {
        chart = new Highcharts.Chart({
            chart: {
                renderTo: '{{ chart_id }}',
                plotBackgroundColor: null,
                plotBorderWidth: null,
                plotShadow: false
            },
            title: {
                text: '{{ chart_title }}'
            },
            tooltip: {
                pointFormat: '{series.name}: <b>{point.percentage}%</b>',
                percentageDecimals: 1
            },
            plotOptions: {
                pie: {
                    allowPointSelect: true,
                    cursor: 'pointer',
                    dataLabels: {
                        enabled: true,
                        color: '#000000',
                        connectorColor: '#000000',
                        formatter: function() {
                          if (this.y < 1024*1024) {
                            return '<b>'+ this.point.name +'</b>: '+ Highcharts.numberFormat(this.percentage, 1) +' % : ' + this.y;
                          } else {
                            K = 1024;
                            exponent = [2,3,4,5];
                            formater = ['M', 'G', 'T', 'P'];
                            var i;
                            for (i= 0; i < exponent.length; i++)
                            {
                              larger_num = Math.pow(K,exponent[i]);
                              if (this.y < larger_num * K) {
                                this.y = (1.0*this.y/larger_num).toFixed(2);
                                break;
                              }
                            }
                            return '<b>'+ this.point.name +'</b>: '+ Highcharts.numberFormat(this.percentage, 1) +' % : ' + this.y + formater[i];
                          }
                        }
                    }
                }
            },
            series: [{
                type: 'pie',
                name: '{{ chart_title }}',
                 events: {
                     click: function(e) {
                        location.href = e.point.url
                        e.preventDefault();
                     }
                 },
                data: [
                    {% for name, info in request_dist.items %}
                      {name:'{{ name }}', y:{{ info.1 }}, url:'{{ base_url }}{{ info.0 }}'},
                    {% endfor %}
                ]
            }]
        });
    });
</script>
<div id="{{ chart_id }}" ></div>
