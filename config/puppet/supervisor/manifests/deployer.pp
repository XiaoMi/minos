class supervisor::deployer {
    # deploy supervisor
    file { "$app/supervisor.tar.gz":
        source => "puppet://$puppetserver/modules/supervisor/supervisor.tar.gz",
        owner => "work",
        group => "work",
        mode => "0755",
        before => Exec["decompression"],
        notify => Exec["decompression"]
    }

    exec { "decompression":
        refreshonly => true,
        command => "tar -xf $app/supervisor.tar.gz -C $app/",
        notify => Class["supervisor::starter"]
    }
}
