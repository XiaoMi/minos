<script>
    $(function () {
        $('#start_datetimepicker').datetimepicker(
                {
                    dateFormat:'yy-mm-dd',
                    timeFormat:'HH-mm',
                    separator:'-'
                }
        )
        $('#end_datetimepicker').datetimepicker(
                {
                    dateFormat:'yy-mm-dd',
                    timeFormat:'HH-mm',
                    separator:'-'
                }
        );
    });
</script>

<form class="form-inline" align="right">
<b>From:</b>
<input type="text" name="start_time" id="start_datetimepicker" value="{{ time_range.0|date:"Y-m-d-H-i" }}"
       class="input-medium"/>
<b>To:</b>
<input type="text" name="end_time" id="end_datetimepicker" value="{{ time_range.1|date:"Y-m-d-H-i" }}"
       class="input-medium"/>
<button type="submit" class="btn">Filter</button>

</form>