class supervisor::starter {
    # start or restarter the supervisord process
    exec { "start supervisord process":
        before => Class["supervisor::monitor"],
        refreshonly => true,
        command => "su - work -c 'cd $app/supervisor/;sh start_supervisor.sh'"
    }
}

