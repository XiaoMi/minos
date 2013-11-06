class supervisor::monitor {
    # monitor the supervisord process
    exec { "monitor supervisord process":
        command => "su - work -c 'cd $app/supervisor/;sh start_supervisor.sh'",
        unless => "ps aux | grep supervisord.py | grep -v grep"
    }
}
