
# Install puppet-master and puppet-client <http://puppetlabs.com/misc/download-options>

# Pack the `supervisor` directory and put it under your puppet-master's module directory:

    ${puppet-master-root}/modules/supervisor/supervisor.tar.gz

# Create the `packages_root`, `app_root`, `log_root` and `data_dirs` directories on the puppet-client machine according to the configuration items in `templates/supervisord.conf.erb`

