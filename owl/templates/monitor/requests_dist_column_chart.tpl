<script type="text/javascript">
$(function () {
    var chart;
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
            xAxis: {
                categories: [
                    {% for item in request_dist %}
                    '{{ item.0 }}', 
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
                    text: 'Requests'
                }
            },
            legend: {
                enabled: false
            },
            tooltip: {
                formatter: function() {
                    return '<b>'+ this.x +'</b><br/>'+
                        'Request Count: '+ this.y;
                }
            },
            series: [{
                name: 'Requests',
                data: [
                    {% for item in request_dist %}
                    {{ item.1 }}, 
                    {% endfor %}
                ],
            }]
        });
    });
});
</script>
<div id="{{ chart_id }}"></div>
