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
                            return '<b>'+ this.point.name +'</b>: '+ Highcharts.numberFormat(this.percentage, 1) +' % : ' + this.y;
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
