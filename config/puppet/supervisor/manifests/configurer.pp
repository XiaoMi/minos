class supervisor::configurer  {
    # supervisor config file
    $data_dirs = "/home/work/data"
    file { "$app/supervisor/supervisord.conf":
        content => template("/etc/puppet/modules/supervisor/templates/supervisord.conf.erb"),
        owner => "work",
        group => "work",
        require => Class["supervisor::deployer"],
        before => Class["supervisor::starter"],
        notify => Class["supervisor::starter"]
    }
}
