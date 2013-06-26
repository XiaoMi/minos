<script type="text/javascript">
$(function () {
    var chart;
    $(document).ready(function() {
        chart = new Highcharts.Chart({
            chart: {
                renderTo: 'chart-{{name}}',
                type: '{{ type }}',
                zoomType: 'xy',
                marginRight: 30,
                marginBottom: 30,
                borderWidth:2
            },
            title: {
                text: '{{ name }}',
                x: -20 //center
            },
            subtitle: {
                text: 'Source: xiaomi.com',
                x: -20
            },
            xAxis: {
                type: 'datetime',
                categories: [ '{{ xAxis.data|join:"','" }}'],
                labels: {
                    step: {{ xAxis.step }}
                }
            },
            yAxis: {
                title: {
                    text: 'Metrics'
                },
                plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            tooltip: {
                formatter: function() {
                    return '<b>'+ this.series.name +'</b><br/>'+
                    this.x +': '+ this.y;
                }
            },
            legend: {
                layout: 'vertical',
                align: 'right',
                verticalAlign: 'top',
                x: -10,
                y: 50,
                borderWidth: 0
            },
            series: [
            {% for yAxis in yAxises %}
            {
               name: '{{yAxis.title}}',
               data: [{{ yAxis.data|join:"," }}]
            },
            {% endfor %}
            ]
        });
    });
});
</script>

<section id="demo">
<h4>{{ name }}</h4>
<div id="chart-{{ name }}" style="height: 400px"></div>
<div align="right"><a href="/admin/monitor/chartconfig/{{ config.id }}/">Edit</a></div>
</section>
